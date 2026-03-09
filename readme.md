## Distributed Experiment Project

A lightweight distributed job queue system with worker heartbeat monitoring.

### Quick Start

**Server:**
```python
from distributed_experiment.server import create_app
import uvicorn

job_ids = list(range(1, 101))
app = create_app(job_ids)
uvicorn.run(app, host="0.0.0.0", port=8000)
```

**Worker:**
```python
from distributed_experiment.worker import Worker

def process_job(job_id: int) -> dict:
    return {"result": job_id * 2}

with Worker() as worker:
    while True:
        job_id = worker.request_job()
        if job_id is None:
            break
        result = process_job(job_id)
        worker.submit_job(job_id, result)
```

### How It Works

1. **Server** initializes with a list of job IDs and persists progress to disk
2. **Workers** request available jobs, process them, and submit results
3. **Dead worker detection** automatically requeues jobs if a worker stops sending heartbeats
4. **Restart safe** — only incomplete jobs are requeued on server restart

### API

**POST /request_jobs** - Get available jobs
- Request: `{"machine_id": int}`
- Response: `{"status": "ok|empty", "job_ids": [...]}`

**POST /submit_jobs** - Submit completed jobs
- Request: `{"machine_id": int, "job_ids": [...], "results": {...}}`
- Response: `{"status": "ok", "completed": N}`

**POST /update_heartbeat** - Keep worker alive
- Request: `{"machine_id": int|null}`
- Response: `{"status": "ok", "machine_id": int}`

### Configuration

**Server:**
```python
create_app(
    job_ids=[1, 2, 3, ...],      # Jobs to process
    db_path="store.db",          # Persistence file
    worker_timeout_seconds=60    # Dead worker timeout
)
```

**Worker:**
```python
Worker(
    server_url="http://localhost:8000",
    machine_id=None,                    # Auto-generated if None
    heartbeat_interval_seconds=5.0,
    request_timeout_seconds=10.0
)
```

### Exposing with Cloudflare Tunnel

Expose the server via Cloudflare tunnel for remote workers without firewall configuration:

**1. Install Cloudflare Tunnel:**
```bash
# macOS/Linux
curl -L --output cloudflared.tgz https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.tgz
tar -xzf cloudflared.tgz
sudo mv cloudflared /usr/local/bin/

# Or use package managers
brew install cloudflare/cloudflare/cloudflared  # macOS
```

**2. Authenticate and create tunnel:**
```bash
cloudflared tunnel login
cloudflared tunnel create my-job-server
```

**3. Configure tunnel (create `~/.cloudflared/config.yml`):**
```yaml
tunnel: my-job-server
credentials-file: /path/to/.cloudflared/<TUNNEL_ID>.json

ingress:
  - hostname: my-job-server.example.com
    service: http://localhost:8000
  - service: http_status:404
```

**4. Start tunnel:**
```bash
cloudflared tunnel run my-job-server
```

**5. Connect workers:**
```python
# Workers connect via tunnel URL (not localhost)
with Worker(server_url="https://my-job-server.example.com") as worker:
    while True:
        job_id = worker.request_job()
        if job_id is None:
            break
        worker.submit_job(job_id, {"result": job_id * 2})
```

See [Cloudflare Tunnel documentation](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/#outbound-only-connections) for advanced options.

### Architecture

- **Store**: Thread-safe shelve-based persistent storage
- **JobQueue**: Manages job distribution and dead worker detection
- **Worker**: Simple client that auto-maintains background heartbeat