"""
Analyze a GitHub repo and report code statistics.

Usage:
    uv run python github_repo_stat.py https://github.com/owner/repo
    uv run python github_repo_stat.py https://github.com/owner/private-repo --token ghp_xxx
    GITHUB_TOKEN=ghp_xxx uv run python github_repo_stat.py https://github.com/owner/private-repo
"""

import argparse
from datetime import datetime
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile


def clone_repo(url: str, dest: str, token: str | None = None) -> None:
    """Clone a git repo to the destination directory."""
    clone_url = url
    if token and url.startswith("https://"):
        # Inject token into URL: https://TOKEN@github.com/owner/repo.git
        clone_url = url.replace("https://", f"https://{token}@", 1)

    # Don't print the token-embedded URL
    print(f"Cloning {url} ...")
    try:
        subprocess.run(
            ["git", "clone", "--quiet", clone_url, dest],
            check=True,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").lower()
        if "not found" in stderr or "does not exist" in stderr:
            raise SystemExit(f"Error: Repository not found: {url}")
        if "could not read username" in stderr or "authentication" in stderr or "invalid username" in stderr:
            hint = " Try passing --token ghp_xxx" if not token else " The provided token may be invalid."
            raise SystemExit(
                f"Error: Could not access {url}\n"
                f"This usually means the repo is private or the URL is incorrect.\n"
                f"{hint}"
            )
        raise
    print("Clone complete.\n")


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
        # Skip .git directory
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


def get_commit_count(repo_dir: str) -> int:
    """Get total number of commits including merge commits."""
    result = subprocess.run(
        ["git", "rev-list", "--count", "HEAD"],
        cwd=repo_dir, capture_output=True, text=True, check=True,
    )
    return int(result.stdout.strip())


def get_commit_stats(repo_dir: str) -> list[int]:
    """Get lines changed (additions + deletions) per commit using git log."""
    result = subprocess.run(
        ["git", "log", "--pretty=format:%H", "--shortstat"],
        cwd=repo_dir,
        capture_output=True,
        text=True,
        check=True,
    )

    lines_per_commit: list[int] = []
    for line in result.stdout.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        # Match shortstat lines like: "3 files changed, 10 insertions(+), 2 deletions(-)"
        insertions = 0
        deletions = 0
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
    """Get the first and last commit dates and the lifespan in days."""
    # Get all commit dates (newest first)
    result = subprocess.run(
        ["git", "log", "--pretty=format:%aI"],
        cwd=repo_dir, capture_output=True, text=True, check=True,
    )
    dates = result.stdout.strip().split("\n")

    last_date = datetime.fromisoformat(dates[0])    # newest
    first_date = datetime.fromisoformat(dates[-1])  # oldest
    lifespan_days = (last_date - first_date).days

    return first_date.strftime("%Y-%m-%d"), last_date.strftime("%Y-%m-%d"), lifespan_days


def format_stats(values: list[int]) -> str:
    """Format min/median/average/max for a list of integers."""
    if not values:
        return "  (no data)"
    return (
        f"  min:     {min(values):,}\n"
        f"  median:  {statistics.median(values):,.1f}\n"
        f"  average: {statistics.mean(values):,.1f}\n"
        f"  max:     {max(values):,}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Analyze a GitHub repo and report code statistics."
    )
    parser.add_argument("url", help="GitHub repo URL (e.g. https://github.com/owner/repo)")
    parser.add_argument(
        "--token", "-t",
        default=os.environ.get("GITHUB_TOKEN"),
        help="GitHub personal access token for private repos (or set GITHUB_TOKEN env var)",
    )
    args = parser.parse_args()

    url = args.url.rstrip("/")
    # Normalize: accept URLs with or without .git
    if not url.endswith(".git"):
        git_url = url + ".git"
    else:
        git_url = url

    tmp_dir = tempfile.mkdtemp(prefix="ghstat_")
    try:
        clone_repo(git_url, tmp_dir, token=args.token)

        # --- File stats ---
        file_stats = get_file_stats(tmp_dir)

        # --- Commit stats ---
        total_commits = get_commit_count(tmp_dir)
        commit_lines = get_commit_stats(tmp_dir)

        # --- Lifespan ---
        first_date, last_date, lifespan_days = get_lifespan(tmp_dir)

        # --- Report ---
        repo_name = url.rstrip("/").split("/")[-1].removesuffix(".git")
        print(f"=== {repo_name} ===\n")

        print(f"Total lines of code:  {file_stats['total_lines']:,}")
        print(f"Number of files:      {file_stats['total_files']:,}")
        print(f"Total commits:        {total_commits:,}")
        print(f"Lifespan:             {lifespan_days:,} days ({first_date} to {last_date})")
        
        loc_per_day = file_stats["total_lines"] / max(lifespan_days, 1)
        print(f"LOC per day:          {loc_per_day:,.1f}")

        print(f"\nLines of code per file:")
        print(format_stats(file_stats["lines_per_file"]))

        print(f"\nLines changed per commit:")
        print(format_stats(commit_lines))

    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr or e}", file=sys.stderr)
        sys.exit(1)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
