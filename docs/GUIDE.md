# Utilities Developer Guide

This document contains technical details, under-the-hood implementations, and architectural notes for the scripts in this repository.

## github_repo_stat.py

### Smart Cloning Architecture
Analyzing very large repositories (e.g., `openai/codex`) can result in excessively long clone times or network timeouts if cloned synchronously. To alleviate this, the script implements a dynamic dual-strategy approach. It initially queries the repository size via the GitHub REST API and picks the optimal path:

1. **Small Repositories (< 75 MB)**:
   - Performs a standard **full `git` clone**.
   - All statistics, including LOC changed per commit, are computed entirely locally using `git log --shortstat`.
   - If the full clone process somehow exceeds a 2-minute (`120s`) timeout, the script wipes the partial clone and falls back to the shallow strategy.

2. **Large Repositories (≥ 75 MB)**:
   - Performs a **shallow clone** (`--depth 1`) solely for computing the active file matrix and total lines of code.
   - Fetches full commit history **without blobs** (`--filter=blob:none`) to rapidly retrieve commit counts and project lifespan without downloading file histories.
   - Bypasses local `git log --shortstat` (which forces extremely slow lazy-fetching of blobs on blobless clones) in favor of fetching per-commit diffs via the **GitHub GraphQL API**. SHAs are batched by 50 and executed across up to 10 parallel threads.

### Environment & Token Loading
The script relies on `python-dotenv` natively to securely manage GitHub personal access tokens.
- It looks for `.env` specifically in the script's directory (`os.path.dirname(__file__)`), ensuring it can be executed from any `CWD` via `uv run`.
- It overrides ambient shells (`override=True`) to prevent stale shell tokens from silently shadowing the expected token in `.env`.
- A valid `GITHUB_TOKEN` is mandatory for the large-repository (GraphQL) fallback logic because GitHub's GraphQL API disallows unauthenticated requests.

### Robust Disk Cleanup
Because repositories can easily be hundreds of megabytes in size, the application ensures that temporary clone directories (`/tmp/ghstat_*`) never exhaust local disk space:
- Normal execution, keyboard interrupts (Ctrl+C), and runtime errors trigger standard cleanup via python `finally` blocks.
- An `atexit` registered handler acts as a backup for unhandled process exits.
- On startup, the script conducts a proactive sweep of the system's temporary directory, scanning for and purging orphaned directories leftover from any past abnormal terminations (like `kill -9` or hard OS crashes).
