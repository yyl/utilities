"""
Analyze a GitHub repo and report code statistics.

Usage:
    uv run python github_repo_stat.py https://github.com/owner/repo
    uv run python github_repo_stat.py https://github.com/owner/private-repo --token ghp_xxx
    GITHUB_TOKEN=ghp_xxx uv run python github_repo_stat.py https://github.com/owner/private-repo
"""

import argparse
import atexit
from datetime import datetime
from dotenv import load_dotenv
import glob
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import urllib.request
import urllib.error

# Repos under this size get a full clone; above get shallow + GraphQL.
FULL_CLONE_THRESHOLD_KB = 75 * 1024  # 75 MB
FULL_CLONE_TIMEOUT = 120  # seconds — fall back to shallow if exceeded


def _make_clone_url(url: str, token: str | None) -> str:
    """Inject token into HTTPS URL if provided."""
    if token and url.startswith("https://"):
        return url.replace("https://", f"https://{token}@", 1)
    return url


def _parse_owner_repo(url: str) -> tuple[str, str] | None:
    """Extract (owner, repo) from a GitHub URL."""
    m = re.match(r"https?://github\.com/([^/]+)/([^/.]+)", url)
    if m:
        return m.group(1), m.group(2)
    return None


def _github_api(endpoint: str, token: str | None, timeout: int = 30) -> dict | list | None:
    """Make a GET request to the GitHub REST API."""
    url = f"https://api.github.com{endpoint}"
    headers = {"Accept": "application/vnd.github+json"}

    # Try with token first, then without (public repos don't need auth)
    for auth_token in ([token, None] if token else [None]):
        h = dict(headers)
        if auth_token:
            h["Authorization"] = f"Bearer {auth_token}"
        req = urllib.request.Request(url, headers=h)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 401 and auth_token:
                continue  # retry without auth
            return None
        except (urllib.error.URLError, json.JSONDecodeError):
            return None
    return None


def _github_graphql(query: str, variables: dict, token: str) -> dict | None:
    """Make a request to the GitHub GraphQL API (requires auth)."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    body = json.dumps({"query": query, "variables": variables}).encode()
    req = urllib.request.Request(
        "https://api.github.com/graphql", data=body, headers=headers, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            result = json.loads(resp.read())
            if "errors" in result:
                return None
            return result.get("data")
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError):
        return None


def get_repo_size_kb(owner: str, repo: str, token: str | None) -> int | None:
    """Get repo size in KB via GitHub API. Returns None on failure."""
    data = _github_api(f"/repos/{owner}/{repo}", token)
    if data and "size" in data:
        return data["size"]
    return None


# ---------------------------------------------------------------------------
# Cloning
# ---------------------------------------------------------------------------

def clone_repo_full(url: str, dest: str, token: str | None = None) -> bool:
    """Full clone (all history + blobs). Returns False if timed out."""
    clone_url = _make_clone_url(url, token)
    print(f"Cloning {url} (full, {FULL_CLONE_TIMEOUT}s timeout) ...")
    try:
        subprocess.run(
            ["git", "clone", "--quiet", clone_url, dest],
            check=True, capture_output=True, text=True,
            timeout=FULL_CLONE_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        print("  Clone timed out, switching to shallow strategy.\n")
        return False
    except subprocess.CalledProcessError as e:
        _handle_clone_error(e, url, token)
    print("Clone complete.\n")
    return True


def clone_repo_shallow(url: str, dest: str, token: str | None = None) -> None:
    """Shallow clone (depth=1, only HEAD tree)."""
    clone_url = _make_clone_url(url, token)
    print(f"Cloning {url} (shallow) ...")
    try:
        subprocess.run(
            ["git", "clone", "--depth", "1", "--quiet", clone_url, dest],
            check=True, capture_output=True, text=True, timeout=300,
        )
    except subprocess.TimeoutExpired:
        raise SystemExit(f"Error: Clone timed out after 300 seconds for {url}")
    except subprocess.CalledProcessError as e:
        _handle_clone_error(e, url, token)
    print("Clone complete.\n")


def fetch_full_history(repo_dir: str) -> None:
    """Unshallow a repo, fetching full commit history without blobs."""
    print("Fetching full commit history (metadata only) ...")
    try:
        subprocess.run(
            ["git", "-C", repo_dir, "fetch", "--unshallow",
             "--filter=blob:none", "--quiet"],
            check=True, capture_output=True, text=True, timeout=300,
        )
    except subprocess.TimeoutExpired:
        raise SystemExit(
            "Error: Fetching commit history timed out after 300 seconds"
        )
    print("History fetched.\n")


def _handle_clone_error(e: subprocess.CalledProcessError, url: str,
                        token: str | None) -> None:
    stderr = (e.stderr or "").lower()
    if "not found" in stderr or "does not exist" in stderr:
        raise SystemExit(f"Error: Repository not found: {url}")
    if any(k in stderr for k in (
        "could not read username", "authentication", "invalid username"
    )):
        hint = (" Try passing --token ghp_xxx" if not token
                else " The provided token may be invalid.")
        raise SystemExit(
            f"Error: Could not access {url}\n"
            f"This usually means the repo is private or the URL is incorrect.\n"
            f"{hint}"
        )
    raise


# ---------------------------------------------------------------------------
# File analysis
# ---------------------------------------------------------------------------

def is_text_file(path: str) -> bool:
    """Heuristic check: try reading a file as UTF-8 text."""
    try:
        with open(path, "r", encoding="utf-8", errors="strict") as f:
            f.read(4096)
        return True
    except (UnicodeDecodeError, PermissionError):
        return False


def get_file_stats(repo_dir: str) -> dict:
    """Walk the repo tree and collect per-file line counts."""
    file_lines: list[int] = []
    total_lines = 0
    total_files = 0

    for root, dirs, files in os.walk(repo_dir):
        dirs[:] = [d for d in dirs if d != ".git"]
        for fname in files:
            path = os.path.join(root, fname)
            if not os.path.isfile(path) or os.path.islink(path):
                continue
            if not is_text_file(path):
                continue
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as f:
                    count = sum(1 for _ in f)
            except (PermissionError, OSError):
                continue
            file_lines.append(count)
            total_lines += count
            total_files += 1

    return {
        "total_lines": total_lines,
        "total_files": total_files,
        "lines_per_file": file_lines,
    }


# ---------------------------------------------------------------------------
# Commit analysis — git
# ---------------------------------------------------------------------------

def get_commit_count(repo_dir: str) -> int:
    result = subprocess.run(
        ["git", "rev-list", "--count", "HEAD"],
        cwd=repo_dir, capture_output=True, text=True, check=True,
    )
    return int(result.stdout.strip())


def get_commit_stats_git(repo_dir: str, timeout: int = 600) -> list[int]:
    """Per-commit lines changed via git log --shortstat. Needs blob data."""
    try:
        result = subprocess.run(
            ["git", "log", "--pretty=format:%H", "--shortstat", "--no-renames"],
            cwd=repo_dir, capture_output=True, text=True, check=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return []

    lines_per_commit: list[int] = []
    for line in result.stdout.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        insertions = deletions = 0
        m_ins = re.search(r"(\d+) insertion", line)
        m_del = re.search(r"(\d+) deletion", line)
        if m_ins:
            insertions = int(m_ins.group(1))
        if m_del:
            deletions = int(m_del.group(1))
        if m_ins or m_del:
            lines_per_commit.append(insertions + deletions)
    return lines_per_commit


def get_lifespan(repo_dir: str) -> tuple[str, str, int]:
    result = subprocess.run(
        ["git", "log", "--pretty=format:%aI"],
        cwd=repo_dir, capture_output=True, text=True, check=True,
    )
    dates = result.stdout.strip().split("\n")
    last_date = datetime.fromisoformat(dates[0])
    first_date = datetime.fromisoformat(dates[-1])
    lifespan_days = (last_date - first_date).days
    return (first_date.strftime("%Y-%m-%d"),
            last_date.strftime("%Y-%m-%d"),
            lifespan_days)


# ---------------------------------------------------------------------------
# Commit analysis — GraphQL
# ---------------------------------------------------------------------------

def _build_batch_query(shas: list[str], owner: str, repo: str) -> str:
    """Build a GraphQL query that fetches stats for multiple commits by SHA."""
    fragments = []
    for i, sha in enumerate(shas):
        fragments.append(
            f'  c{i}: object(oid: "{sha}") {{ ... on Commit {{ additions deletions }} }}'
        )
    body = "\n".join(fragments)
    return (
        f'query {{\n'
        f'  repository(owner: "{owner}", name: "{repo}") {{\n'
        f'{body}\n'
        f'  }}\n'
        f'}}'
    )


def _fetch_batch(shas: list[str], owner: str, repo: str,
                 token: str) -> list[int]:
    """Fetch commit stats for a batch of SHAs via GraphQL. Returns lines changed."""
    query = _build_batch_query(shas, owner, repo)
    data = _github_graphql(query, {}, token)
    if not data or "repository" not in data:
        return []

    results = []
    repo_data = data["repository"]
    for i in range(len(shas)):
        node = repo_data.get(f"c{i}")
        if node:
            total = node.get("additions", 0) + node.get("deletions", 0)
            if total > 0:
                results.append(total)
    return results


def get_commit_stats_graphql(owner: str, repo: str, token: str,
                             repo_dir: str) -> list[int]:
    """Fetch per-commit diff stats via parallel GraphQL batch queries.

    Gets all commit SHAs from the local repo (blobless clone), then queries
    GitHub in parallel batches of 50 SHAs using GraphQL aliases.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Get all commit SHAs from local repo (instant on blobless clone)
    result = subprocess.run(
        ["git", "log", "--format=%H"],
        cwd=repo_dir, capture_output=True, text=True, check=True,
    )
    all_shas = result.stdout.strip().split("\n")
    if not all_shas or all_shas == [""]:
        return []

    # Chunk into batches of 50 (safe limit for GraphQL query complexity)
    batch_size = 50
    batches = [all_shas[i:i + batch_size]
               for i in range(0, len(all_shas), batch_size)]

    lines_per_commit: list[int] = []
    completed = 0

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {
            pool.submit(_fetch_batch, batch, owner, repo, token): batch
            for batch in batches
        }
        for future in as_completed(futures):
            results = future.result()
            lines_per_commit.extend(results)
            completed += 1
            if completed % 20 == 0 or completed == len(batches):
                print(f"    ... {completed}/{len(batches)} batches "
                      f"({len(lines_per_commit):,} commits)")

    return lines_per_commit


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def format_stats(values: list[int]) -> str:
    if not values:
        return "  (no data)"
    return (
        f"  min:     {min(values):,}\n"
        f"  median:  {statistics.median(values):,.1f}\n"
        f"  average: {statistics.mean(values):,.1f}\n"
        f"  max:     {max(values):,}"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"),
                override=True)
    parser = argparse.ArgumentParser(
        description="Analyze a GitHub repo and report code statistics."
    )
    parser.add_argument(
        "url", help="GitHub repo URL (e.g. https://github.com/owner/repo)")
    parser.add_argument(
        "--token", "-t",
        default=os.environ.get("GITHUB_TOKEN"),
        help="GitHub personal access token (or set GITHUB_TOKEN env var)",
    )
    args = parser.parse_args()

    url = args.url.rstrip("/")
    git_url = url if url.endswith(".git") else url + ".git"
    owner_repo = _parse_owner_repo(url)

    # --- Clean up leftover temp dirs from previous crashed runs ---
    for stale in glob.glob(os.path.join(tempfile.gettempdir(), "ghstat_*")):
        try:
            shutil.rmtree(stale)
        except OSError:
            pass

    # --- Determine clone strategy based on repo size ---
    try_full_clone = True  # default for non-GitHub URLs or API failure
    repo_size_kb = None
    if owner_repo:
        owner, repo = owner_repo
        repo_size_kb = get_repo_size_kb(owner, repo, args.token)
        if repo_size_kb is not None:
            size_mb = repo_size_kb / 1024
            print(f"Repo size: {size_mb:,.0f} MB")
            try_full_clone = repo_size_kb < FULL_CLONE_THRESHOLD_KB
            if not try_full_clone:
                print(f"  Over {FULL_CLONE_THRESHOLD_KB // 1024} MB threshold "
                      f"→ using shallow + GraphQL\n")

    tmp_dir = tempfile.mkdtemp(prefix="ghstat_")
    atexit.register(shutil.rmtree, tmp_dir, True)  # backup cleanup
    try:
        did_full_clone = False
        if try_full_clone:
            did_full_clone = clone_repo_full(git_url, tmp_dir, token=args.token)
            if not did_full_clone:
                # Timed out — wipe partial clone and redo as shallow
                shutil.rmtree(tmp_dir, ignore_errors=True)
                os.makedirs(tmp_dir, exist_ok=True)

        if did_full_clone:
            # ---- Full clone succeeded: everything local ----
            print("Analyzing files ...")
            file_stats = get_file_stats(tmp_dir)
            print(f"  Found {file_stats['total_files']:,} text files.\n")

            total_commits = get_commit_count(tmp_dir)
            first_date, last_date, lifespan_days = get_lifespan(tmp_dir)

            print("Analyzing commit diffs ...")
            commit_lines = get_commit_stats_git(tmp_dir)
            if commit_lines:
                print(f"  Analyzed {len(commit_lines):,} commits.\n")
            else:
                print("  No commit diff data available.\n")

        else:
            # ---- Shallow clone + blobless history + GraphQL ----
            clone_repo_shallow(git_url, tmp_dir, token=args.token)

            print("Analyzing files ...")
            file_stats = get_file_stats(tmp_dir)
            print(f"  Found {file_stats['total_files']:,} text files.\n")

            # Fetch commit metadata (blobless — just trees & commits)
            fetch_full_history(tmp_dir)
            total_commits = get_commit_count(tmp_dir)
            first_date, last_date, lifespan_days = get_lifespan(tmp_dir)

            # Per-commit diff stats via GraphQL (no blob data needed)
            commit_lines = []
            if args.token and owner_repo:
                print("Fetching per-commit diff stats via GitHub API ...")
                commit_lines = get_commit_stats_graphql(
                    owner_repo[0], owner_repo[1], args.token, tmp_dir
                )
                if commit_lines:
                    print(f"  Fetched {len(commit_lines):,} commits.\n")
                else:
                    print("  Could not retrieve commit diff stats.\n")
            else:
                print("Skipping per-commit diff stats (requires --token "
                      "for large repos).\n")

        # --- Report ---
        repo_name = url.rstrip("/").split("/")[-1].removesuffix(".git")
        loc_per_day = file_stats["total_lines"] / max(lifespan_days, 1)
        size_line = (f"Repo size:            {repo_size_kb / 1024:,.0f} MB"
                     if repo_size_kb is not None else
                     "Repo size:            (unknown)")

        report = "\n".join([
            f"=== {repo_name} ===",
            "",
            size_line,
            f"Total lines of code:  {file_stats['total_lines']:,}",
            f"Number of files:      {file_stats['total_files']:,}",
            f"Total commits:        {total_commits:,}",
            f"Lifespan:             {lifespan_days:,} days "
            f"({first_date} to {last_date})",
            f"LOC per day:          {loc_per_day:,.1f}",
            "",
            "Lines of code per file:",
            format_stats(file_stats["lines_per_file"]),
            "",
            "Lines changed per commit:",
            format_stats(commit_lines),
        ])

        print(report)

        # --- Write to file ---
        today = datetime.now().strftime("%Y-%m-%d")
        owner_name = owner_repo[0] if owner_repo else "unknown"
        stats_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "stats"
        )
        os.makedirs(stats_dir, exist_ok=True)
        stats_file = os.path.join(
            stats_dir, f"{owner_name}-{repo_name}-{today}.txt"
        )
        with open(stats_file, "w", encoding="utf-8") as f:
            f.write(report + "\n")
        print(f"\nSaved to {stats_file}")

    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr or e}", file=sys.stderr)
        sys.exit(1)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
