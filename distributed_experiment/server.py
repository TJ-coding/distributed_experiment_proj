from __future__ import annotations

from typing import Optional, Any
from contextlib import asynccontextmanager
import shelve
import threading
import time

from fastapi import FastAPI, Body, HTTPException
from fastapi.responses import HTMLResponse
from tqdm import tqdm


def format_duration(seconds: float | None) -> str | None:
    """Format a duration in seconds as a compact human-readable string."""
    if seconds is None:
        return None

    total_seconds = max(0, int(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)

    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    if minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


class Store:
    """Simple persistent storage using shelve."""

    def __init__(self, db_path: str = "store.db") -> None:
        self.db_path = db_path
        self._db = None
        self._lock = threading.Lock()

    def open(self) -> None:
        with self._lock:
            if self._db is None:
                self._db = shelve.open(self.db_path, writeback=True)

    def close(self) -> None:
        with self._lock:
            if self._db is not None:
                self._db.sync()
                self._db.close()
                self._db = None

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            if self._db is None:
                raise RuntimeError("Database is not initialized")
            return self._db.get(key, default)

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            if self._db is None:
                raise RuntimeError("Database is not initialized")
            self._db[key] = value
            self._db.sync()


class JobQueue:
    """Simple job queue manager."""

    def __init__(self, store: Store, worker_timeout_seconds: int = 60, batch_size: int = 10):
        self.store = store
        self.worker_timeout_seconds = worker_timeout_seconds
        self.batch_size = batch_size
        self.progress_bar: Optional[tqdm] = None
        self.run_started_at: Optional[float] = None
        self.run_started_completed_jobs = 0

    def request_jobs(self, machine_id: int) -> list[int]:
        """Request up to batch_size available jobs for a worker."""
        self._reconcile_dead_workers()

        queue = self.store.get("job_queue", [])
        if not queue:
            return []

        # Take only batch_size jobs from the queue
        job_ids = queue[:self.batch_size]
        self.store.set("job_queue", queue[self.batch_size:])

        assigned = self.store.get("assigned_jobs", [])
        for job_id in job_ids:
            assigned.append({"machine_id": machine_id, "job_id": job_id})
        self.store.set("assigned_jobs", assigned)

        return job_ids

    def submit_jobs(self, machine_id: int, job_ids: list[int]) -> int:
        """Mark jobs as completed."""
        self._reconcile_dead_workers()

        assigned = self.store.get("assigned_jobs", [])
        job_id_set = set(job_ids)
        kept = []
        completed_count = 0

        for item in assigned:
            if item["machine_id"] == machine_id and item["job_id"] in job_id_set:
                completed_count += 1
            else:
                kept.append(item)

        self.store.set("assigned_jobs", kept)
        
        # Track completed jobs (use list for shelve compatibility)
        completed = set(self.store.get("completed_jobs", []))
        completed.update(job_ids)
        self.store.set("completed_jobs", list(completed))
        
        # Update progress bar
        if self.progress_bar is not None:
            self.progress_bar.update(completed_count)
        
        return completed_count

    def update_heartbeat(self, machine_id: Optional[int]) -> int:
        """Update worker heartbeat and return assigned machine_id."""
        self._reconcile_dead_workers()

        heartbeats = self.store.get("heartbeats", {})
        if machine_id is None:
            machine_id = len(heartbeats) + 1

        heartbeats[str(machine_id)] = time.time()
        self.store.set("heartbeats", heartbeats)
        return int(machine_id)

    def _reconcile_dead_workers(self) -> None:
        """Detect dead workers and requeue their jobs."""
        heartbeats = self.store.get("heartbeats", {})
        now = time.time()

        dead_workers = [
            int(mid) for mid, ts in heartbeats.items()
            if now - ts > self.worker_timeout_seconds
        ]

        if not dead_workers:
            return

        # Requeue jobs from dead workers
        assigned = self.store.get("assigned_jobs", [])
        dead_set = set(dead_workers)
        to_requeue = [item["job_id"] for item in assigned if item["machine_id"] in dead_set]

        assigned = [item for item in assigned if item["machine_id"] not in dead_set]
        self.store.set("assigned_jobs", assigned)

        if to_requeue:
            queue = self.store.get("job_queue", [])
            queue.extend(to_requeue)
            self.store.set("job_queue", queue)

        # Remove dead workers from heartbeat map
        for mid in dead_workers:
            heartbeats.pop(str(mid), None)
        self.store.set("heartbeats", heartbeats)

    def get_status(self, total_jobs: int) -> dict[str, int | float | str | None]:
        """Return current queue status for display/monitoring."""
        self._reconcile_dead_workers()

        completed_jobs = len(set(self.store.get("completed_jobs", [])))
        live_workers = len(self.store.get("heartbeats", {}))
        percent_completed = 0.0
        if total_jobs > 0:
            percent_completed = completed_jobs / total_jobs * 100

        elapsed_seconds = None
        elapsed_display = None
        eta_seconds = None
        eta_display = None
        if self.run_started_at is not None:
            elapsed_seconds = time.time() - self.run_started_at
            elapsed_display = format_duration(elapsed_seconds)

            completed_this_run = completed_jobs - self.run_started_completed_jobs
            remaining_jobs = max(0, total_jobs - completed_jobs)
            if completed_this_run > 0 and elapsed_seconds > 0 and remaining_jobs > 0:
                jobs_per_second = completed_this_run / elapsed_seconds
                eta_seconds = remaining_jobs / jobs_per_second
                eta_display = format_duration(eta_seconds)
            elif remaining_jobs == 0:
                eta_seconds = 0.0
                eta_display = format_duration(eta_seconds)

        return {
            "completed_jobs": completed_jobs,
            "total_jobs": total_jobs,
            "percent_completed": percent_completed,
            "live_workers": live_workers,
            "elapsed_seconds": elapsed_seconds,
            "elapsed_display": elapsed_display,
            "eta_seconds": eta_seconds,
            "eta_display": eta_display,
        }


def create_app(job_ids: list[int], db_path: str = "store.db", worker_timeout_seconds: int = 60, batch_size: int = 10) -> FastAPI:
    """Create FastAPI app with job queue.
    
    Args:
        job_ids: List of job IDs to process. On startup, completed jobs are filtered out.
        db_path: Path to persistent storage database.
        worker_timeout_seconds: Time before marking worker as dead.
        batch_size: Maximum number of jobs to return per request.
    """
    store = Store(db_path)
    queue = JobQueue(store, worker_timeout_seconds, batch_size)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store.open()
        
        # Get completed jobs (stored as list, convert to set for fast lookup)
        completed = set(store.get("completed_jobs", []))
        
        # Clear assigned jobs on restart (workers may be dead)
        store.set("assigned_jobs", [])
        store.set("heartbeats", {})
        
        # Reinitialize queue: all jobs minus completed only
        pending_jobs = [jid for jid in job_ids if jid not in completed]
        store.set("job_queue", pending_jobs)
        
        # Calculate progress
        total_jobs = len(job_ids)
        completed_count = len(completed)
        queue.run_started_at = time.time()
        queue.run_started_completed_jobs = completed_count
        queue.progress_bar = tqdm(total=total_jobs, initial=completed_count, desc="Jobs")
        
        yield
        
        # Close progress bar
        if queue.progress_bar is not None:
            queue.progress_bar.close()
        store.close()

    app = FastAPI(lifespan=lifespan)

    @app.post("/request_jobs")
    def request_jobs_endpoint(payload: dict = Body(default_factory=dict)):
        machine_id_raw = payload.get("machine_id")
        if machine_id_raw is None:
            raise HTTPException(status_code=400, detail="machine_id is required")

        machine_id = int(machine_id_raw)
        job_ids_result = queue.request_jobs(machine_id)

        if not job_ids_result:
            return {"status": "empty", "job_ids": []}
        return {"status": "ok", "job_ids": job_ids_result}

    @app.post("/submit_jobs")
    def submit_jobs_endpoint(payload: dict = Body(default_factory=dict)):
        machine_id_raw = payload.get("machine_id")
        job_ids_raw = payload.get("job_ids")
        if machine_id_raw is None or not isinstance(job_ids_raw, list):
            raise HTTPException(status_code=400, detail="machine_id and job_ids list are required")

        if not isinstance(machine_id_raw, int) or isinstance(machine_id_raw, bool):
            raise HTTPException(
                status_code=400,
                detail=(
                    "machine_id must be an integer. "
                    "Hint: this request likely came from a legacy worker/client payload."
                ),
            )

        if any(isinstance(jid, list) for jid in job_ids_raw):
            raise HTTPException(
                status_code=400,
                detail=(
                    "job_ids must be a flat list of integers (e.g. [1, 2, 3], not [[1, 2, 3]]). "
                    "Hint: worker/client looks legacy (double-wrapping job_ids before submit)."
                ),
            )

        if any(not isinstance(jid, int) or isinstance(jid, bool) for jid in job_ids_raw):
            raise HTTPException(
                status_code=400,
                detail=(
                    "job_ids must contain integers only. "
                    "Hint: request format may be from a legacy worker/client."
                ),
            )

        machine_id = machine_id_raw
        job_ids_list = job_ids_raw

        completed = queue.submit_jobs(machine_id, job_ids_list)

        return {"status": "ok", "completed": completed}

    @app.post("/update_heartbeat")
    def update_heartbeat(payload: dict = Body(default_factory=dict)):
        machine_id = payload.get("machine_id")
        assigned_machine_id = queue.update_heartbeat(machine_id)
        return {"status": "ok", "machine_id": assigned_machine_id}

    @app.get("/status")
    def status():
        status = queue.get_status(len(job_ids))
        completed_jobs = status["completed_jobs"]
        total_jobs = status["total_jobs"]
        percent_completed = status["percent_completed"]
        live_workers = status["live_workers"]
        elapsed_display = status["elapsed_display"]
        eta_display = status["eta_display"]
        return {
            "message": (
                f"Jobs Completed / Total Jobs: {completed_jobs}/{total_jobs}. "
                f"% Completed: {percent_completed:.1f}%. "
                f"Live Workers Connected: {live_workers}. "
                f"Time Elapsed: {elapsed_display or 'n/a'}. "
                f"ETA: {eta_display or 'n/a'}"
            ),
            "completed_jobs": completed_jobs,
            "total_jobs": total_jobs,
            "percent_completed": percent_completed,
            "live_workers": live_workers,
            "elapsed_seconds": status["elapsed_seconds"],
            "elapsed_display": elapsed_display,
            "eta_seconds": status["eta_seconds"],
            "eta_display": eta_display,
        }

    @app.get("/", response_class=HTMLResponse)
    def root_status():
        status = queue.get_status(len(job_ids))
        completed_jobs = status["completed_jobs"]
        total_jobs = status["total_jobs"]
        percent_completed = status["percent_completed"]
        live_workers = status["live_workers"]
        elapsed_display = status["elapsed_display"] or "n/a"
        eta_display = status["eta_display"] or "n/a"

        progress_width = min(100.0, max(0.0, float(percent_completed)))

        return f"""
<!DOCTYPE html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\">
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
    <meta http-equiv=\"refresh\" content=\"5\">
    <title>Distributed Experiment Status</title>
    <style>
        :root {{
            color-scheme: light;
            --bg: #f4efe6;
            --panel: #fffdf8;
            --ink: #1f2328;
            --muted: #5b6470;
            --accent: #0b6e4f;
            --accent-soft: #d8efe7;
            --border: #d7d0c4;
        }}
        * {{ box-sizing: border-box; }}
        body {{
            margin: 0;
            font-family: Georgia, "Times New Roman", serif;
            color: var(--ink);
            background:
                radial-gradient(circle at top left, #fff8e8 0, transparent 35%),
                linear-gradient(180deg, #efe7da 0%, var(--bg) 100%);
            min-height: 100vh;
        }}
        .wrap {{
            max-width: 860px;
            margin: 0 auto;
            padding: 40px 20px 56px;
        }}
        .panel {{
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 18px;
            padding: 28px;
            box-shadow: 0 14px 40px rgba(48, 41, 30, 0.08);
        }}
        h1 {{
            margin: 0 0 8px;
            font-size: 2rem;
            line-height: 1.1;
        }}
        p {{
            margin: 0;
            color: var(--muted);
            font-size: 1rem;
        }}
        .progress {{
            margin: 24px 0 28px;
            width: 100%;
            height: 18px;
            background: #ece4d7;
            border-radius: 999px;
            overflow: hidden;
            border: 1px solid #ddd2c2;
        }}
        .progress-bar {{
            height: 100%;
            width: {progress_width:.1f}%;
            background: linear-gradient(90deg, #1f8f68 0%, var(--accent) 100%);
            transition: width 0.3s ease;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            overflow: hidden;
            border-radius: 12px;
            border: 1px solid var(--border);
        }}
        th, td {{
            padding: 14px 16px;
            border-bottom: 1px solid var(--border);
            text-align: left;
            font-size: 1rem;
        }}
        th {{
            width: 40%;
            background: #f7f1e8;
            font-weight: 700;
        }}
        tr:last-child th,
        tr:last-child td {{
            border-bottom: none;
        }}
        .pill {{
            display: inline-block;
            margin-top: 16px;
            padding: 8px 12px;
            background: var(--accent-soft);
            color: var(--accent);
            border-radius: 999px;
            font: 600 0.9rem/1.2 Arial, sans-serif;
        }}
        code {{
            font-family: "SFMono-Regular", Consolas, monospace;
            font-size: 0.9rem;
        }}
    </style>
</head>
<body>
    <main class=\"wrap\">
        <section class=\"panel\">
            <h1>Distributed Experiment Status</h1>
            <p>Auto-refreshes every 5 seconds. Machine-readable status is available at <code>/status</code>.</p>
            <div class=\"progress\" aria-label=\"Progress\">
                <div class=\"progress-bar\"></div>
            </div>
            <table>
                <tr><th>Jobs Completed / Total Jobs</th><td>{completed_jobs}/{total_jobs}</td></tr>
                <tr><th>Percent Completed</th><td>{percent_completed:.1f}%</td></tr>
                <tr><th>Live Workers Connected</th><td>{live_workers}</td></tr>
                <tr><th>Time Elapsed</th><td>{elapsed_display}</td></tr>
                <tr><th>ETA</th><td>{eta_display}</td></tr>
            </table>
            <div class=\"pill\">Batch size: {queue.batch_size}</div>
        </section>
    </main>
</body>
</html>
"""

    return app


if __name__ == "__main__":
    import uvicorn

    # Example: create app with jobs 1-100
    app = create_app(list(range(1, 101)))
    uvicorn.run(app, host="0.0.0.0", port=8000)
