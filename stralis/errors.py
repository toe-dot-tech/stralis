"""Stralis exception hierarchy."""

from __future__ import annotations


class StralisError(Exception):
    """Base class for all Stralis errors."""


class ConfigError(StralisError):
    """Invalid or missing configuration."""


class PipelineValidationError(StralisError):
    """A pipeline spec failed compilation (cycles, missing deps, ...)."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("pipeline validation failed: " + "; ".join(errors))


class QueueError(StralisError):
    """Task queue backend failure."""


class StorageError(StralisError):
    """Object store backend failure."""


class UnknownHandlerError(StralisError):
    """A node references a handler that is not registered."""


class HandlerExecutionError(StralisError):
    """A node handler failed to execute."""
