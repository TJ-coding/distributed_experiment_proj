from __future__ import annotations

from typing import Optional
import threading
import time

import requests
from tqdm import tqdm


class Worker:
    """Simple worker client for requesting/submitting jobs."""

    def __init__(
        self,
        server_url: str = "http://127.0.0.1:8000",
        machine_id: Optional[int] = None,
        heartbeat_interval_seconds: float = 5.0,
        request_timeout_seconds: float = 10.0,
        show_progress: bool = True,
    ) -> None:
        self.base_url = server_url.rstrip("/")
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.request_timeout_seconds = request_timeout_seconds
        self.show_progress = show_progress
        self._progress_bar: Optional[tqdm] = None

        if machine_id is None:
            machine_id = int(time.time() * 1000) % 2_000_000_000

        self.machine_id = int(machine_id)
        self._session = requests.Session()
        self._stop_event = threading.Event()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"worker-heartbeat-{self.machine_id}",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def __enter__(self) -> Worker:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - stops heartbeat."""
        self.close()

    def close(self) -> None:
        """Stop heartbeat and close session."""
        self._stop_event.set()
        self._heartbeat_thread.join(timeout=self.request_timeout_seconds)
        self._session.close()
        if self._progress_bar is not None:
            self._progress_bar.close()

    def request_job(self) -> list[int]:
        """Request a batch of job IDs from the server. Returns empty list if no jobs available.
        
        Returns a list of job IDs (e.g., [1, 2, 3, 4, 5]). The batch size is configured on the server.
        """
        response = self._session.post(
            f"{self.base_url}/request_jobs",
            json={"machine_id": self.machine_id},
            timeout=self.request_timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()

        job_ids = payload.get("job_ids", [])
        if not isinstance(job_ids, list):
            raise RuntimeError(
                "Invalid /request_jobs response: expected 'job_ids' as list[int]. "
                "Hint: server may be using a legacy response format."
            )
        if any(not isinstance(jid, int) or isinstance(jid, bool) for jid in job_ids):
            raise RuntimeError(
                "Invalid /request_jobs response: 'job_ids' must contain integers only. "
                "Hint: server may be using a legacy response format."
            )
        
        # Initialize progress bar on first batch
        if job_ids and self._progress_bar is None and self.show_progress:
            self._progress_bar = tqdm(desc=f"Worker {self.machine_id}", unit="jobs")
        
        return job_ids

    def submit_jobs(self, job_ids: list[int]) -> None:
        """Submit multiple completed job IDs."""
        if not isinstance(job_ids, list):
            raise TypeError("submit_jobs expects list[int]")
        if any(not isinstance(jid, int) or isinstance(jid, bool) for jid in job_ids):
            raise TypeError("submit_jobs expects list[int] with integer job IDs only")

        response = self._session.post(
            f"{self.base_url}/submit_jobs",
            json={"machine_id": self.machine_id, "job_ids": job_ids},
            timeout=self.request_timeout_seconds,
        )
        response.raise_for_status()
        
        # Update progress bar
        if self._progress_bar is not None:
            self._progress_bar.update(len(job_ids))

    def _heartbeat_loop(self) -> None:
        """Background heartbeat thread."""
        while not self._stop_event.is_set():
            try:
                response = self._session.post(
                    f"{self.base_url}/update_heartbeat",
                    json={"machine_id": self.machine_id},
                    timeout=self.request_timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()

                # Server may assign/correct machine_id.
                machine_id = payload.get("machine_id")
                if machine_id is not None:
                    self.machine_id = int(machine_id)
            except requests.RequestException:
                # Keep worker alive; next loop retries heartbeat.
                pass

            self._stop_event.wait(self.heartbeat_interval_seconds)
