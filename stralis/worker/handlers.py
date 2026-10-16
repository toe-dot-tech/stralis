"""Built-in node handlers.

A handler is a callable ``(NodeContext) -> dict[str, Path]`` mapping artifact
names to local files that the worker uploads to the object store. Handlers
must be idempotent: given the same inputs they must produce the same outputs,
because the engine may re-execute a node after a crash (checkpoint resume).
"""

from __future__ import annotations

import os
import re
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from stralis.errors import HandlerExecutionError

Handler = Callable[["NodeContext"], dict[str, Path]]


@dataclass
class NodeContext:
    """Everything a handler needs to run one node attempt."""

    job_id: str
    node_id: str
    attempt: int
    params: dict[str, Any]
    inputs: dict[str, Path]
    workdir: Path
    log: Any = None  # optional logger (duck-typed)

    def output_path(self, name: str) -> Path:
        """A stable path for a declared output artifact."""
        safe = name.replace("/", "_").replace("\\", "_")
        p = self.workdir / "outputs" / safe
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def require_input(self, name: str) -> Path:
        try:
            return self.inputs[name]
        except KeyError:
            raise HandlerExecutionError(
                f"node {self.node_id!r} requires input {name!r}; "
                f"available: {sorted(self.inputs)}"
            ) from None

    def _log(self, msg: str) -> None:
        if self.log is not None:
            self.log.info(msg)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

HANDLERS: dict[str, Handler] = {}


def register(name: str) -> Callable[[Handler], Handler]:
    def deco(fn: Handler) -> Handler:
        HANDLERS[name] = fn
        return fn

    return deco


def get_handler(name: str) -> Handler:
    try:
        return HANDLERS[name]
    except KeyError:
        from stralis.errors import UnknownHandlerError

        raise UnknownHandlerError(
            f"unknown handler {name!r}; registered: {sorted(HANDLERS)}"
        ) from None


# ---------------------------------------------------------------------------
# noop — deterministic tiny artifact (great for demos/tests)
# ---------------------------------------------------------------------------


@register("noop")
def noop(ctx: NodeContext) -> dict[str, Path]:
    """Write ``params.content`` (default a deterministic marker) to each output."""
    outputs = _declared_outputs(ctx)
    content = ctx.params.get("content", f"noop:{ctx.job_id}:{ctx.node_id}\n")
    data = content.encode("utf-8")
    written: dict[str, Path] = {}
    for name in outputs:
        p = ctx.output_path(name)
        p.write_bytes(data)
        written[name] = p
    return written


# ---------------------------------------------------------------------------
# shell — run an arbitrary command with input/output placeholders
# ---------------------------------------------------------------------------


@register("shell")
def shell(ctx: NodeContext) -> dict[str, Path]:
    """Run ``params.command`` (list of tokens) with placeholder substitution.

    Tokens may contain ``{input:NAME}`` (local path of a parent artifact) and
    ``{output:NAME}`` (local path this node must write). Declared
    ``params.outputs`` must exist afterwards and are uploaded by the worker.
    """
    command = ctx.params.get("command")
    if not command:
        raise HandlerExecutionError("shell handler requires params.command")
    if isinstance(command, str):
        command = shlex.split(command)

    resolved: list[str] = []
    output_paths = {name: ctx.output_path(name) for name in _declared_outputs(ctx)}
    for token in command:
        resolved.append(_substitute(token, ctx, output_paths))

    timeout = float(ctx.params.get("timeout", 3600))
    env = {**os.environ, **{str(k): str(v) for k, v in ctx.params.get("env", {}).items()}}

    ctx._log(f"shell: {' '.join(resolved)}")
    try:
        proc = subprocess.run(
            resolved,
            cwd=str(ctx.workdir),
            env=env,
            timeout=timeout,
            capture_output=True,
        )
    except FileNotFoundError as e:
        raise HandlerExecutionError(f"command not found: {resolved[0]!r} ({e})") from e
    except subprocess.TimeoutExpired as e:
        raise HandlerExecutionError(
            f"command timed out after {timeout}s"
        ) from e

    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or b"").decode("utf-8", "replace")[-2000:]
        raise HandlerExecutionError(
            f"command exited {proc.returncode}: {tail.strip()}"
        )

    return _collect_outputs(ctx, output_paths)


# ---------------------------------------------------------------------------
# ffmpeg.transcode — thin, ergonomic wrapper over the ffmpeg CLI
# ---------------------------------------------------------------------------


@register("ffmpeg.transcode")
def ffmpeg_transcode(ctx: NodeContext) -> dict[str, Path]:
    """``ffmpeg -y -i <input> <args...> <output>``.

    params:
      input:  parent artifact name (default: the sole/first input)
      output: artifact name to produce (default: "video.mp4")
      args:   ffmpeg argv between the input and output
              (default ["-c:v","libx264","-preset","veryfast","-c:a","aac"])
      timeout: seconds (default 3600)
    """
    if not shutil.which("ffmpeg"):
        raise HandlerExecutionError("ffmpeg binary not found on PATH")

    out_name = ctx.params.get("output", "video.mp4")
    if ctx.params.get("input"):
        src = ctx.require_input(ctx.params["input"])
    elif ctx.inputs:
        src = next(iter(ctx.inputs.values()))
    else:
        raise HandlerExecutionError("ffmpeg.transcode requires an input artifact")

    out_path = ctx.output_path(out_name)
    args = [str(a) for a in ctx.params.get("args", ["-c:v", "libx264", "-preset", "veryfast", "-c:a", "aac"])]
    timeout = float(ctx.params.get("timeout", 3600))

    resolved = ["ffmpeg", "-y", "-i", str(src), *args, str(out_path)]
    ctx._log(f"ffmpeg: {' '.join(resolved)}")
    try:
        proc = subprocess.run(resolved, timeout=timeout, capture_output=True)
    except subprocess.TimeoutExpired as e:
        raise HandlerExecutionError(f"ffmpeg timed out after {timeout}s") from e
    if proc.returncode != 0:
        tail = (proc.stderr or b"").decode("utf-8", "replace")[-2000:]
        raise HandlerExecutionError(f"ffmpeg exited {proc.returncode}: {tail.strip()}")

    return {out_name: out_path}


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _declared_outputs(ctx: NodeContext) -> list[str]:
    outputs = ctx.params.get("outputs", [])
    if isinstance(outputs, str):
        outputs = [outputs]
    return [str(o) for o in outputs]


def _substitute(token: str, ctx: NodeContext, output_paths: dict[str, Path]) -> str:
    def repl(match) -> str:
        kind, name = match.group(1), match.group(2)
        if kind == "input":
            return str(ctx.require_input(name))
        return str(output_paths.setdefault(name, ctx.output_path(name)))

    return re.sub(r"\{(input|output):([^}]+)\}", repl, token)


def _collect_outputs(ctx: NodeContext, output_paths: dict[str, Path]) -> dict[str, Path]:
    written: dict[str, Path] = {}
    for name, path in output_paths.items():
        if not path.exists():
            raise HandlerExecutionError(
                f"declared output {name!r} was not produced at {path}"
            )
        written[name] = path
    return written


def default_handlers() -> dict[str, Handler]:
    """The registry used by the worker runtime (copy so callers can extend)."""
    return dict(HANDLERS)


__all__ = [
    "Handler",
    "NodeContext",
    "HANDLERS",
    "register",
    "get_handler",
    "default_handlers",
    "noop",
    "shell",
    "ffmpeg_transcode",
]
