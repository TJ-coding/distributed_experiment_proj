from __future__ import annotations

import typer
import uvicorn

from distributed_experiment.server import create_app


app = typer.Typer(
    help="CLI utilities for distributed-experiment.",
    no_args_is_help=True,
)


@app.command("serve")
def serve(
    jobs: int = typer.Option(
        10,
        "--jobs",
        min=1,
        help="Total number of job IDs to generate using list(range(jobs)).",
    ),
    host: str = typer.Option("0.0.0.0", "--host", help="Host interface to bind."),
    port: int = typer.Option(8000, "--port", min=1, max=65535, help="Port to bind."),
    db_path: str = typer.Option("store.db", "--db-path", help="Path to shelve DB file."),
    worker_timeout_seconds: int = typer.Option(
        60,
        "--worker-timeout-seconds",
        min=1,
        help="Seconds before a worker is treated as dead.",
    ),
    batch_size: int = typer.Option(
        10,
        "--batch-size",
        min=1,
        help="Maximum jobs returned per worker request.",
    ),
) -> None:
    """Start the API server with generated IDs: list(range(jobs))."""
    job_ids = list(range(jobs))
    server_app = create_app(
        job_ids=job_ids,
        db_path=db_path,
        worker_timeout_seconds=worker_timeout_seconds,
        batch_size=batch_size,
    )
    uvicorn.run(server_app, host=host, port=port)


def main() -> None:
	"""Entry point for console scripts."""
	app()


if __name__ == "__main__":
    main()
