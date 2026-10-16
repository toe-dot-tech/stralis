# Stralis

**Distributed media automation engine** — compiles multi-step pipelines into resilient, fault-tolerant DAGs.

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/)
[![CI](https://github.com/toe-dot-tech/stralis/actions/workflows/ci.yml/badge.svg)](https://github.com/toe-dot-tech/stralis/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Submit a pipeline spec (JSON) to the API, get `202 Accepted` + a `job_id` immediately, and let a pool of
workers grind through the DAG asynchronously. Media moves through an S3-compatible object store; only
lightweight references cross the queue. Postgres (or SQLite locally) is the single source of truth for
job and node state, with heartbeat-based crash recovery and checkpoint resume at the failed node.

---

## What it is

Stralis is a case study in fixing three failure modes of "just run it in the request handler" media
pipelines:

| Problem | Symptom |
| --- | --- |
| **HTTP timeout wall** | A transcode that takes minutes blows past the gateway's request timeout, and the client sees an error while the work (or a half-done version of it) is still running. |
| **State-machine failures** | Long pipelines keep their progress in memory or in a single worker process; a crash mid-way loses everything and forces a full restart. |
| **Message-queue bloat** | Pushing whole media files through the broker (RabbitMQ/Kafka payloads, base64 blobs) saturates broker memory and disk. |

Stralis answers with three mechanisms:

1. **DAG compilation.** A submitted `PipelineSpec` is validated and compiled into a directed acyclic
   graph. Nodes with no unfinished parents are *ready*; readiness is derived from persisted state, not
   from an in-memory scheduler.
2. **Async worker pools + `202`.** The API only validates, compiles and persists the job. It returns
   `202 Accepted` + `job_id` in milliseconds; arbitrarily long node execution happens behind the queue.
3. **Claim-check messaging.** Handlers read/write media through an S3-compatible object store. Queue
   messages carry only `{job_id, node_id, attempt}` — a few bytes — never media bytes.

## Architecture

```
                    POST /api/v1/pipelines (PipelineSpec JSON)
   ┌──────────┐  ────────────────────────────────────────────►  ┌───────────────┐
   │  client  │                                                 │  API (202)    │
   │          │  ◄────────────────────────────────────────────   │  FastAPI      │
   └──────────┘       { "job_id": "...", "status": ... }        └───────┬───────┘
        ▲                                                              │ compile DAG
        │  GET /api/v1/jobs/{job_id}                                  ▼
        │                                                    ┌────────────────────┐
        └──────────────────────────────────────────────────► │  Postgres / SQLite │  source of truth
                                                             │  jobs              │  (state machine)
                                                             │  node_runs         │
                                                             │  artifacts         │
                                                             └───▲────────────┬───┘
                                              heartbeat/claim    │            │ promote READY
                                                                 │            ▼
   ┌────────────────┐      XADD ready nodes      ┌────────────────────────────────┐
   │  orchestrator  │ ─────────────────────────► │  Redis Streams (consumer group)│
   │  (DAG walker)  │ ◄───────────────────────── │  duplicate/loss tolerant       │
   └────────────────┘      reclaim stale         └───────────────▲────────────────┘
                                                                  │ XREADGROUP
                                                   ┌──────────────┴───────────────┐
                                                   │  worker pool (N replicas)    │
                                                   │  noop · shell · ffmpeg ...   │
                                                   └───┬──────────────────────┬───┘
                                                       │ PUT/GET media        │ heartbeat
                                                       ▼                      ▼
                                                ┌────────────┐        ┌────────────┐
                                                │   MinIO    │        │  Postgres  │
                                                │ (claim-    │        │ (fencing,  │
                                                │  check)    │        │  checkpts) │
                                                └────────────┘        └────────────┘
```

- **API** compiles + persists the job (all root nodes `READY`) and returns `202`.
- **Orchestrator** repeatedly promotes `PENDING → READY` nodes, enqueues READY nodes, and reclaims
  nodes whose worker stopped heartbeating.
- **Workers** claim a node (fenced by `attempt`), download parent artifacts from the object store,
  run the handler, upload outputs, and mark the node terminal.
- **Postgres/SQLite** remains authoritative at every step; Redis is only a hint.

### Repository layout

```text
stralis/
├── stralis/
│   ├── models.py          # PipelineSpec / NodeSpec / JobRecord / NodeRunRecord (pydantic)
│   ├── dag/compiler.py    # validation + deterministic Kahn topological order
│   ├── db/                # SQLAlchemy ORM, engine, and the Repo (fencing, checkpoints, resume)
│   ├── ports/             # TaskQueue + ObjectStore protocols (the seams)
│   ├── queue/             # adapters: redis_streams.py, sqlite_queue.py
│   ├── storage/           # adapters: s3.py, local.py
│   ├── worker/            # worker loop + handlers (noop, shell, ffmpeg.transcode)
│   ├── orchestrator/      # engine.py: sweep → promote → enqueue → recover (the only scheduler)
│   ├── api/app.py         # FastAPI control plane
│   ├── cli.py             # typer CLI (submit/status/list/retry/worker/orchestrator/serve)
│   └── runtime.py         # backend selection from Config (redis|sqlite, s3|local)
├── ui_flutter/            # Flutter (Dart) web/desktop client
├── examples/              # demo_dag.json, video_transcode.json, audio_transcribe.json
├── tests/                 # unit + local-mode integration; redis/s3 suites skip if unconfigured
├── Dockerfile             # api/orchestrator/worker image (ffmpeg baked in)
└── docker-compose.yml     # redis + minio + postgres + the three app roles
```

### Benchmarks

Representative figures from the case study (1 worker vs. the naive synchronous gateway), intended to
show the shape of the improvement rather than precise numbers:

| Dimension | Synchronous gateway (naive) | Stralis |
| --- | --- | --- |
| **Max task duration** | `< 60s` (HTTP timeout wall) | **Unbounded** (202 + async workers) |
| **Failure blast radius** | Entire pipeline | **Isolated node** |
| **Broker memory / 1k jobs** | ~2.5 TB (inline media) | **~12 MB** (claim-check URIs) |
| **Pipeline resumption** | 100% restart penalty | **0%** — resume at the failed node |

## Quickstart with Docker Compose

Requires Docker Compose v2 (Compose Spec; no `version:` key).

```bash
# 1. Bring up Redis, MinIO, Postgres and the three app roles (api/orchestrator/worker).
docker compose up -d --build

# 2. Wait for the API, then submit an example pipeline.
curl -X POST http://localhost:8000/api/v1/pipelines \
  -H 'content-type: application/json' \
  --data-binary @examples/demo_dag.json
# -> {"job_id":"<uuid>","name":"demo-diamond","status":"pending"}

# 3. Poll the job (replace <uuid> with the value above).
curl http://localhost:8000/api/v1/jobs/<uuid>

# 4. Watch the workers.
docker compose logs -f worker
docker compose logs -f orchestrator
```

`examples/demo_dag.json` is a 4-node diamond that runs with the standard-library `shell` handler only —
no ffmpeg, no external media — so it succeeds on the base image. MinIO's browser console is at
<http://localhost:9001> (user `minio` / password `minio123`); submitted artifacts land in bucket
`stralis`.

To enable the media examples (`examples/video_transcode.json`, `examples/audio_transcribe.json`) see
[Media toolkit layer](#media-toolkit-layer) below.

Tear down (add `-v` to drop the Postgres/MinIO volumes):

```bash
docker compose down
```

## Local dev without Docker

The defaults are SQLite + `redis://localhost:6379/0`; no Postgres is required.

```bash
python -m venv .venv
. .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -e ".[dev]"       # runtime deps + postgres extra + pytest
```

The database defaults to `sqlite:///./stralis.db`, so the API can accept and compile specs with zero
external services. To run a job end to end you still need a Redis (Streams) and an S3-compatible
endpoint — point `STRALIS_REDIS_URL` / `STRALIS_S3_*` at local instances if you have them:

```bash
# Terminal 1 — HTTP API
stralis serve --host 127.0.0.1 --port 8000

# Terminal 2 — orchestrator (needs Redis)
stralis orchestrator

# Terminal 3 — worker (needs Redis + S3)
stralis worker

# Run the test suite
pytest
```

Run the installed `stralis` console script (or `python -m stralis.cli`) and set
`STRALIS_LOG_LEVEL=DEBUG` for verbose tracing.

### Zero-services local mode

If you have neither Redis nor MinIO (or just want the fastest possible inner loop), Stralis ships a
self-contained mode that swaps **both** ports for in-tree implementations: a SQLite-backed `TaskQueue`
and a filesystem `ObjectStore`. Nothing else is required — no broker, no object store, no Postgres.

```powershell
$env:STRALIS_DATABASE_URL = "sqlite:///./.demo/stralis.db"
$env:STRALIS_QUEUE_BACKEND = "sqlite"
$env:STRALIS_STORE_BACKEND = "local"
$env:STRALIS_LOCAL_STORE_ROOT = "./.demo/objects"
$env:STRALIS_WORK_ROOT = "./.demo/work"

python -m stralis submit examples/demo_dag.json   # prints: job_id <uuid>
python -m stralis status <uuid> --watch           # poll until terminal
```

Then run the full HTTP + orchestrator + worker stack in three terminals with the same environment:
`uvicorn stralis.api.app:app`, `python -m stralis orchestrator`, `python -m stralis worker`.

The backend selection is pure configuration resolved by `stralis.runtime`; the engine, repo and
resilience guarantees are identical either way, which is exactly what the ports/adapters split buys.
The CI suite exercises this mode end to end.

### Media toolkit layer

The base image is `python:3.12-slim` and does **not** ship ffmpeg. The `ffmpeg.transcode` handler and the
ffmpeg-based `shell` nodes in the examples require the binary. The provided `Dockerfile` already bakes in
this layer:

```dockerfile
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
```

Remove that layer to slim the image for pure-Python/shell pipelines. For Whisper transcription
(`examples/audio_transcribe.json`) you additionally need the Python package in the worker image — the
base image does not include it; add e.g. `pip install openai-whisper` to the `Dockerfile` build. Whisper
is user-provided and not a Stralis dependency.

`pyproject.toml` declares `requires-python = ">=3.11"`. The Dockerfile pins `python:3.12-slim` for broad
availability; swap it for `python:3.14-slim` once GA images are used in your environment.

## Testing & CI

The suite is `pytest` (Python) plus `flutter test` (Dart UI). Locally:

```bash
pip install -e ".[dev]"

pytest                                          # fast path; Redis/S3 suites skip if unconfigured
pytest --cov=stralis --cov-report=term-missing  # coverage (config lives in pyproject.toml)
```

Two integration suites are opt-in — they skip unless pointed at real services:

| Suite | Env vars that enable it |
| --- | --- |
| `tests/test_queue_redis.py` | `STRALIS_TEST_REDIS_URL` (e.g. `redis://localhost:6379/0`) |
| `tests/test_storage_s3.py` | `STRALIS_TEST_S3_ENDPOINT_URL`, `STRALIS_TEST_S3_ACCESS_KEY`, `STRALIS_TEST_S3_SECRET_KEY` (optional `STRALIS_TEST_S3_BUCKET`, default `stralis-test`) |

The S3 store provisions its bucket lazily on first write, so a bare MinIO is enough.

The UI has its own suite:

```bash
cd ui_flutter
flutter pub get
flutter analyze
flutter test
```

**CI** (`.github/workflows/ci.yml`) runs on every push to `main` and every pull request:

- **`pytest`** on Python 3.11 and 3.12 against a Redis service container and a MinIO
  container, so the integration suites run instead of skipping; the coverage report is
  uploaded as a build artifact.
- **`flutter analyze` + `flutter test`** for `ui_flutter/`.

## Pipeline spec reference

A pipeline is a JSON object validated against `stralis.models.PipelineSpec`:

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `name` | `str` | — (required) | Human-readable pipeline name. |
| `nodes` | `list[NodeSpec]` | — (required, non-empty) | The DAG's nodes. |
| `max_attempts` | `int` | `3` | Default retry budget per node (must be `>= 1`). |
| `metadata` | `dict` | `{}` | Free-form annotations (description, author, requirements …). |

Each `NodeSpec`:

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `id` | `str` | — (required) | Unique within the pipeline; must match `[A-Za-z0-9_.-]+`. |
| `handler` | `str` | — (required) | Name of a registered handler. |
| `name` | `str \| null` | `null` | Optional human-readable label. |
| `params` | `dict` | `{}` | Handler-specific parameters (see below). |
| `depends_on` | `list[str]` | `[]` | Parent node ids; the node becomes ready once all parents succeed. |
| `max_attempts` | `int \| null` | `null` | Per-node override of the pipeline retry budget. |

Compilation validates the graph (unique ids, known parents, acyclicity); roots (no `depends_on`) start
`READY`, all other nodes start `PENDING`.

### Built-in handlers

**`noop`** — trivial producer used for wiring and tests.

| Param | Type | Default | Meaning |
| --- | --- | --- | --- |
| `outputs` | `list[str]` | — (required) | Artifact names to produce. |
| `content` | `str` | `""` | Text written to each output file. |

**`shell`** — run an external command. Cross-platform; handy for ffmpeg/ffprobe/CLI tooling.

| Param | Type | Default | Meaning |
| --- | --- | --- | --- |
| `command` | `list[str]` | — (required) | Command and arguments. |
| `outputs` | `list[str]` | `[]` | Each named output file **must exist** after the command runs. |
| `timeout` | `float` | `3600` | Seconds before the command is killed. |
| `env` | `dict[str, str]` | `{}` | Extra environment variables. |

Tokens in `command` may contain placeholders:

- `{input:NAME}` → local path of **parent artifact** `NAME`.
- `{output:NAME}` → local path the node must write; parent directories are auto-created.

A non-zero exit status fails the node with the tail of stderr in the node error.

**`ffmpeg.transcode`** — thin wrapper over an ffmpeg transcode.

| Param | Type | Default | Meaning |
| --- | --- | --- | --- |
| `input` | `str` | — | Parent artifact name; **required** unless `args` supplies its own `-i`. |
| `output` | `str` | `"output.mp4"` | Artifact name produced. |
| `args` | `list[str]` | `["-c:v","libx264","-preset","veryfast","-c:a","aac"]` | ffmpeg arguments. |
| `timeout` | `float` | `3600` | Seconds before ffmpeg is killed. |

It runs `ffmpeg -y -i <input path> <args...> <output path>`.

For a **generator** node with no parent input (e.g. the synthetic clip in
`examples/video_transcode.json`), use the `shell` handler with an ffmpeg command that writes
`{output:NAME}` instead.

### Registering custom handlers

Register a handler in code against the `stralis.worker.handlers` registry (map a `handler` name to a
callable that receives the node context and returns its named output paths); the orchestrator and
workers pick it up by name.

## HTTP API reference

Base URL `http://localhost:8000`. Request/response bodies are JSON.

| Method | Path | Purpose | Success | Errors |
| --- | --- | --- | --- | --- |
| `POST` | `/api/v1/pipelines` | Submit a `PipelineSpec`; compile → persist → return immediately. | `202` `{job_id, name, status}` | `422` invalid/uncompilable spec |
| `GET` | `/api/v1/jobs` | List recent jobs (`?limit=&offset=`). | `200` `[job, ...]` | — |
| `GET` | `/api/v1/jobs/{job_id}` | Job detail plus per-node runs. | `200` `{job, node_runs}` | `404` unknown job |
| `GET` | `/api/v1/jobs/{job_id}/artifacts` | Persisted artifact rows (`name`, `uri`, `digest`, `size`). | `200` `[artifact, ...]` | `404` unknown job |
| `POST` | `/api/v1/jobs/{job_id}/retry` | Resume a **failed** job; succeeded nodes keep their checkpoints, failed/skipped nodes re-run. | `202` job | `404` unknown; `409` not failed |
| `GET` | `/healthz` | Liveness probe. | `200` `{"status": "ok"}` | — |

Status codes:

- **`202 Accepted`** — the job was accepted for asynchronous execution; poll `GET /api/v1/jobs/{job_id}`.
- **`404 Not Found`** — no job with that id.
- **`409 Conflict`** — the request is valid but the job is in the wrong state (e.g. retrying a job that is
  not `failed`).
- **`422 Unprocessable Entity`** — the body failed schema validation or DAG compilation
  (`{"detail": {"errors": [ ... ]}}`).

Job status is one of `pending`, `running`, `succeeded`, `failed`, `cancelled`; node status is one of
`pending`, `ready`, `running`, `succeeded`, `failed`, `skipped`.

## CLI reference

Installed as the `stralis` console script.

| Command | Description |
| --- | --- |
| `stralis submit <spec.json>` | Validate + submit a pipeline spec; prints the new `job_id`. |
| `stralis status <job_id> [--watch]` | Print a job and its node runs; `--watch` polls until terminal. |
| `stralis list [--limit N]` | List recent jobs. |
| `stralis retry <job_id>` | Resume a failed job from its failed node. |
| `stralis worker` | Run a worker loop (claim → execute → heartbeat → complete). |
| `stralis orchestrator` | Run the orchestrator loop (sweep → promote → enqueue → recover). |
| `stralis serve [--host H] [--port P]` | Serve the HTTP API (uvicorn). |

`stralis serve` is equivalent to the compose command
`uvicorn stralis.api.app:app --host 0.0.0.0 --port 8000`.

## Web UI (Flutter)

`ui_flutter/` is a Flutter (Dart) client — the same codebase targets web, desktop and mobile. It talks
only to the public HTTP API, so it works against any Stralis deployment (including zero-services local
mode).

- **Dashboard** (`/`) — recent jobs with live status chips (auto-refreshes every 3s).
- **Job detail** (`/jobs/:id`) — an interactive DAG (pan/zoom) whose nodes are colored by live
  `NodeRun` status, plus the per-node run table. Polls while the job is non-terminal and stops at
  `succeeded`/`failed`/`cancelled`.
- **Pipeline editor** (`/editor`) — paste a `PipelineSpec` JSON and submit it.

```bash
cd ui_flutter
flutter pub get
dart run build_runner build --delete-conflicting-outputs   # generates *.g.dart models

# Point the UI at your API (default: http://127.0.0.1:8100) and run it.
flutter run -d chrome --dart-define=STRALIS_API_URL=http://127.0.0.1:8100
```

The API sends permissive CORS headers by default so browser-based development works out of the box;
lock this down with `STRALIS_CORS_ORIGINS` before exposing the API publicly.

State management is Riverpod; routing is `go_router`; the DAG is rendered with `graphview`
(Buchheim–Walker tree layout) inside an `InteractiveViewer`.

## Configuration

All configuration is read from `STRALIS_*` environment variables (see `.env.example`).

| Variable | Default | Meaning |
| --- | --- | --- |
| `STRALIS_DATABASE_URL` | `sqlite:///./stralis.db` | SQLAlchemy DSN. Use `postgresql+psycopg://…` for Postgres (requires the `postgres` extra). |
| `STRALIS_QUEUE_BACKEND` | `redis` | `redis` (Streams) or `sqlite` (in-tree, zero-services). |
| `STRALIS_STORE_BACKEND` | `s3` | `s3` (S3/MinIO) or `local` (filesystem). |
| `STRALIS_LOCAL_STORE_ROOT` | `./.stralis-store` | Root directory when `STRALIS_STORE_BACKEND=local`. |
| `STRALIS_REDIS_URL` | `redis://localhost:6379/0` | Redis used for Streams. |
| `STRALIS_QUEUE_GROUP` | `stralis-workers` | Redis consumer group name shared by workers. |
| `STRALIS_S3_ENDPOINT_URL` | *(none)* | S3-compatible endpoint (e.g. `http://minio:9000`). Empty ⇒ AWS S3 default chain. |
| `STRALIS_S3_BUCKET` | `stralis` | Bucket for all artifacts. |
| `STRALIS_S3_ACCESS_KEY` | `stralis` | S3 access key id. |
| `STRALIS_S3_SECRET_KEY` | `stralis` | S3 secret access key. |
| `STRALIS_S3_REGION` | `us-east-1` | S3 region. |
| `STRALIS_HEARTBEAT_INTERVAL_S` | `5.0` | Seconds between worker heartbeats while a node is `running`. |
| `STRALIS_HEARTBEAT_TIMEOUT_S` | `60.0` | A `running` node whose heartbeat is older than this is presumed dead and requeued. |
| `STRALIS_SWEEP_INTERVAL_S` | `1.0` | Orchestrator sweep cadence (promote / enqueue / recover). |
| `STRALIS_MAX_ATTEMPTS` | `3` | Default per-node retry budget. |
| `STRALIS_WORK_ROOT` | `./.stralis-work` | Worker scratch directory for claim-check downloads before handlers run. |
| `STRALIS_LOG_LEVEL` | `INFO` | Python logging level. |
| `STRALIS_CORS_ORIGINS` | `*` | Comma-separated browser origins allowed to call the API; set explicitly when exposing the UI. |

## Resilience model

- **Attempt fencing.** Every node run carries an `attempt` counter. Transitions to `running`/`succeeded`
  only apply when the persisted attempt matches the worker's — a stale worker whose node was already
  recovered gets zero affected rows and discards its result instead of corrupting state.
- **Heartbeat timeout + requeue.** While a node runs, the worker heartbeats at
  `STRALIS_HEARTBEAT_INTERVAL_S`. The orchestrator treats a `running` node whose heartbeat is older than
  `STRALIS_HEARTBEAT_TIMEOUT_S` as dead and requeues it (or fails it once `STRALIS_MAX_ATTEMPTS` is
  exhausted), again fenced by the heartbeat timestamp so a live worker is never stolen from.
- **Idempotent node hash checkpoint.** Each node execution is identified by a content hash of
  `(handler, params, sorted input digests)`. Re-delivering an already-completed node is detected by its
  hash and its outputs reused, so pipelines resume **at the failed node** rather than from the start.
- **The DB is the source of truth.** Redis Streams may duplicate or drop messages: duplicates are
  dropped at claim time (the `ready → running` transition is conditional), and READY nodes whose queue
  message was lost are re-enqueued by the orchestrator sweep. Losing the broker never loses a job.

## Limitations

- **Single orchestrator.** v0 recommends running **one** orchestrator. State transitions are conditional,
  so multiple orchestrators are largely safe, but coordination overhead is unproven — scale workers, not
  orchestrators.
- **No authentication.** The HTTP API is unauthenticated; put it behind a gateway/auth proxy for anything
  beyond a trusted network.
- **UI is a thin client.** The Flutter app (`ui_flutter/`) covers job monitoring, DAG visualization and
  submission; it is a v0 dashboard, not a full studio (no auth, no artifact preview/download yet).
