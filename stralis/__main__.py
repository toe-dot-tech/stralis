"""Allow ``python -m stralis <command>`` alongside the ``stralis`` console script."""

from stralis.cli import app

if __name__ == "__main__":
    app()
