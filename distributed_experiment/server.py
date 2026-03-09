from __future__ import annotations

from typing import Optional, Any
from contextlib import asynccontextmanager
import shelve
import threading
import time

from fastapi import FastAPI, Body, HTTPException
from tqdm import tqdm


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

    app = FastAPI()

    @app.lifespan("startup")
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store.open()
        
        # Initialize job queue on first run or reload
        existing_queue = store.get("job_queue", [])
        assigned = store.get("assigned_jobs", [])
        
        # Get all job IDs that are still pending or assigned
        pending_job_ids = set(existing_queue)
        for item in assigned:
            pending_job_ids.add(item["job_id"])
        
        # Add only jobs that aren't already pending
        jobs_to_add = [jid for jid in job_ids if jid not in pending_job_ids]
        if jobs_to_add:
            store.set("job_queue", list(pending_job_ids) + jobs_to_add)
        
        # Initialize progress bar
        total_jobs = len(job_ids)
        completed_jobs = total_jobs - len(existing_queue) - len(assigned)
        queue.progress_bar = tqdm(total=total_jobs, initial=completed_jobs, desc="Jobs")
        
        yield
        
        # Close progress bar
        if queue.progress_bar is not None:
            queue.progress_bar.close()
        store.close()

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

        machine_id = int(machine_id_raw)
        job_ids_list = [int(jid) for jid in job_ids_raw]
        completed = queue.submit_jobs(machine_id, job_ids_list)

        return {"status": "ok", "completed": completed}

    @app.post("/update_heartbeat")
    def update_heartbeat(payload: dict = Body(default_factory=dict)):
        machine_id = payload.get("machine_id")
        assigned_machine_id = queue.update_heartbeat(machine_id)
        return {"status": "ok", "machine_id": assigned_machine_id}

    return app


if __name__ == "__main__":
    import uvicorn

    # Example: create app with jobs 1-100
    app = create_app(list(range(1, 101)))
    uvicorn.run(app, host="0.0.0.0", port=8000)
