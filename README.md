# cms-hospitals

Incremental daily ingestion of CMS datasets under `theme=Hospitals`. Each run fetches the CMS metastore, classifies each dataset as **NEW / MODIFIED / UNCHANGED / FAILED** against locally stored state, downloads the changed ones in parallel, rewrites CSV headers to `snake_case`, and records per-dataset + per-run metadata in SQLite.

Single-file implementation in [`src/cms_hospitals/pipeline.py`](src/cms_hospitals/pipeline.py). Requires Python 3.12+.

---

## Quick start

```bash
# requires uv — https://docs.astral.sh/uv/
uv sync
uv run python -m cms_hospitals --dry-run   # sanity: classify without downloading
uv run python -m cms_hospitals             # real run: pull NEW + MODIFIED datasets
```

Sample output: [sample_output/](sample_output/) — two snake-cased CSVs trimmed to 10 rows each.

**Pip fallback:**
```bash
python -m venv .venv && . .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -r requirements.txt                  # runtime
pip install -r requirements-dev.txt              # + tests/lint
pip install -e .
python -m cms_hospitals --dry-run
```

---

## Running

```bash
uv run python -m cms_hospitals --limit N       # partial: process first N datasets
uv run python -m cms_hospitals --full-refresh  # ignore state; re-download everything
```

Exit code is **0** on full success, **1** if any dataset's download/transform FAILED. For scheduled use, invoke `uv run python -m cms_hospitals` from cron / systemd timer / Task Scheduler; the process manager handles log persistence.

Example crontab (daily at 06:00, logs appended to a file):
```
0 6 * * * cd /opt/cms-hospitals && /usr/local/bin/uv run python -m cms_hospitals >> /var/log/cms-hospitals.log 2>&1
```

---

## Testing

```bash
uv run pytest                           # all tests
uv run pytest -m "not integration"      # unit only
uv run pytest -m integration            # end-to-end scenarios
uv run pytest -v                        # verbose
```

---

## What it does

```
1. Fetch metastore JSON (httpx GET, tenacity retries 5xx + timeouts)
2. Filter: [d for d in datasets if "Hospitals" in d.theme]
3. Load state: {identifier: last_good_modified} from SQLite (excluding FAILED rows)
4. Classify each dataset:
     - identifier unknown       → NEW
     - known, modified changed  → MODIFIED
     - known, modified matches  → UNCHANGED (skip)
5. Submit NEW + MODIFIED to ThreadPoolExecutor:
     - stream CSV to per-worker staging path
     - transform: rewrite with snake_case headers (dedup on collisions)
     - move to final output path: output/{YYYY-MM-DD}/{identifier}__{basename}
6. Main thread: consume futures via as_completed, write state rows, tally
7. Write pipeline_runs row with final status
8. Exit 0 on full success, 1 on any FAILED datasets
```

**Steady-state behavior:**
- Day 1 cold run: every discovered dataset is NEW and downloaded.
- Day 2+: UNCHANGED datasets are skipped after the metastore fetch — no CSV download, no disk write. Only NEW + MODIFIED trigger a download.

---

## CLI flags

Run `uv run python -m cms_hospitals --help` for the full list.

Flags marked **Persisted** are written to a JSON sidecar
(`.cms_hospitals_state.config.json`) next to the state DB and reused on
subsequent runs. Non-persisted flags apply only to the current invocation.

| Flag | Meaning | Persisted? |
|---|---|---|
| `--dry-run` | Discover + classify, do not download. Writes a `pipeline_runs` row but not `dataset_runs` rows. | No |
| `--full-refresh` | Ignore stored state; treat every dataset as NEW. | No |
| `--limit N` | Process at most N datasets. | No |
| `--workers N` | Max worker threads (1-16). Default 4. | Yes |
| `--timeout S` | HTTP timeout in seconds. Default 30. | Yes |
| `--output-dir` | Where snake-cased CSVs are written. Default `output/`. | Yes |
| `--state-db` | Path to the SQLite state database. Default `.cms_hospitals_state.db`. | Yes |
| `--staging-dir` | Staging dir for in-progress downloads. Default `.staging/`. | Yes |
| `--theme` | Metastore theme filter. Default `Hospitals`. | Yes |
| `--log-level` | `DEBUG` / `INFO` / `WARNING` / `ERROR`. Default `INFO`. | Yes |

---

## Configuration model

**Precedence:** CLI flag > JSON sidecar > built-in default.

Persistent overrides (e.g. `--workers 8`) are written to a sidecar next to the state DB: `.cms_hospitals_state.config.json`. The next invocation reuses them without re-specifying. Per-run flags (`--dry-run`, `--full-refresh`, `--limit`) are never persisted — they apply only to the invocation that uses them.

**Reset to defaults:** `rm .cms_hospitals_state.config.json`. Next run uses code defaults.

---

## Code organization

The whole pipeline lives in `src/cms_hospitals/pipeline.py`, section-headed top-to-bottom by dependency depth: **Models → Normalize → Classify → Config → State → API → Worker → Orchestrator → Logging → CLI**. Each section is marked with a `# === Section Name ================` banner; `grep -n '^# === '` gives you the table of contents.

---

## State

SQLite file (default `.cms_hospitals_state.db`):
- `dataset_runs` — one row per dataset: identifier, title, download URL, last source `modified` date, downloaded-at timestamp, output path, row count, last run id, last status.
- `pipeline_runs` — one row per invocation: run id, started-at / finished-at, tally (discovered / new / modified / unchanged / failed), final status.

Overlapping runs are prevented by a PID lock at `.cms_hospitals_state.lock`. If a second invocation finds a live lock, it aborts with a message pointing to the file; stale locks (from a process that died) are silently taken over.

---

## Workers

Default **4**, hard cap **16** (enforced at CLI parse time by `click.IntRange(1, 16)`). The work is I/O-bound, so raising the count helps only up to the point your network saturates — past that, more workers gain nothing. Raise `MAX_WORKERS` in `pipeline.py` if you have a reason to go higher.
