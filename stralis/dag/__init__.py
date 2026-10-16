"""DAG compilation: turn a validated PipelineSpec into an executable graph."""

from stralis.dag.compiler import CompiledDAG, compile_pipeline

__all__ = ["CompiledDAG", "compile_pipeline"]
