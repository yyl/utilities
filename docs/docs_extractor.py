"""docs_extractor — Extract main content from developer documentation URLs.

Fetches a URL, looks up a CSS selector from selectors.yaml, extracts the
content, and converts it to clean Markdown for agent consumption.

Usage:
    uv run docs_extractor.py <url> [--config SELECTORS.yaml] [--output DIR]

If the URL doesn't match any selector in the config, the script exits with
an error listing the available patterns.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import click
import markdownify
import requests
import yaml
from bs4 import BeautifulSoup


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class SelectorConfig:
    """A single URL-pattern → CSS-selector mapping."""
    pattern: str
    selector: str


DEFAULT_CONFIG = Path(__file__).parent / "selectors.yaml"
DEFAULT_OUTPUT_DIR = Path(__file__).parent / "docs"


def load_config(path: Path) -> list[SelectorConfig]:
    """Load and parse the selectors YAML file."""
    if not path.exists():
        click.echo(f"Warning: config file not found at {path}", err=True)
        return []

    text = path.read_text(encoding="utf-8")
    data = yaml.safe_load(text)
    if not data or "selectors" not in data:
        return []

    return [SelectorConfig(s["pattern"], s["selector"]) for s in data["selectors"]]


def find_selector(url: str, selectors: list[SelectorConfig]) -> SelectorConfig | None:
    """Find the best matching selector for a URL.

    Matches by checking if the selector's pattern substring exists in the
    URL's hostname. Returns the first (highest priority) match.
    """
    hostname = urlparse(url).hostname or ""
    for cfg in selectors:
        if cfg.pattern.lower() in hostname.lower():
            return cfg
    return None


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; DocsExtractor/0.1; +https://github.com/placeholder/docs-extractor)"
    ),
}


def fetch_html(url: str, timeout: int = 30) -> str:
    """Fetch HTML from a URL, returning the response text."""
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def _remove_noisy_tags(html: str) -> str:
    """Remove script/style and nav/footer elements from HTML before conversion.

    These elements add noise to extracted documentation — CSS, JS, and
    page chrome — without contributing to the actual content.
    """
    soup = BeautifulSoup(html, "html.parser")

    # 1. Hard-remove script/style/noscript (never part of docs content)
    for tag_name in ("script", "noscript", "style"):
        for tag in soup.find_all(tag_name):
            tag.decompose()

    # 2. Remove elements that are clearly page chrome
    for tag in soup.find_all(attrs={"aria-label": lambda v: v and ("navigation" in v.lower() or "footer" in v.lower())}):
        tag.decompose()
    for tag in soup.find_all(attrs={"role": lambda v: v and ("navigation" in v.lower() or "footer" in v.lower())}):
        tag.decompose()
    for tag in soup.find_all(attrs={"data-testid": lambda v: v and "footer" in v.lower()}):
        tag.decompose()

    return str(soup)


def extract_html_to_md(html: str, css_selector: str) -> str:
    """Extract content via a CSS selector and convert to Markdown.

    1. Parse HTML with BeautifulSoup
    2. Find the matching element
    3. Remove noisy tags (script/style/nav/footer)
    4. Convert the element's inner HTML to Markdown
    """
    soup = BeautifulSoup(html, "html.parser")
    element = soup.select_one(css_selector)

    if element is None:
        raise ValueError(
            f"Selector '{css_selector}' did not match any element. "
            "This URL may need a new entry in selectors.yaml."
        )

    # Remove noisy tags, then convert to Markdown
    safe_html = _remove_noisy_tags(str(element))
    md = markdownify.markdownify(
        safe_html,
        heading_style="ATX",
        code_language="",
    )
    return md


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

# Footer phrases to strip from the bottom of output
_FOOTER_PHRASES = [
    "did this page help you?",
    "updated ",
    "getting started",
    "migration guide",
    "related docs",
    "see also",
]


def clean_markdown(md: str) -> str:
    """Clean up raw Markdown output for better agent readability.

    - Removes excessive blank lines (collapses to max 2 consecutive)
    - Strips trailing whitespace from each line
    - Strips footer/engagement text at the bottom
    - Adds a final newline
    """
    lines = md.splitlines()
    cleaned = []
    blank_count = 0

    for line in lines:
        stripped = line.rstrip()

        if stripped == "":
            blank_count += 1
            if blank_count <= 2:
                cleaned.append("")
        else:
            blank_count = 0
            cleaned.append(stripped)

    result = "\n".join(cleaned)

    # Strip footer noise from the bottom (scan upward from end)
    lines = result.splitlines()
    footer_cutoff = len(lines)  # default: keep everything
    for i in range(len(lines) - 1, -1, -1):
        line = lines[i].strip().lower()
        if line in ("yes", "no", "") or line == "---" or any(frase in line for frase in _FOOTER_PHRASES):
            footer_cutoff = i
        else:
            break  # Found real content, stop scanning

    result = "\n".join(lines[:footer_cutoff]).rstrip() + "\n"
    return result


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def output_path(url: str, output_dir: Path) -> Path:
    """Derive a filename from the URL.

    Example: https://docs.foursquare.com/a/b/c
    → docs/docs.foursquare.com_a-b-c.md
    """
    parsed = urlparse(url)
    hostname = parsed.hostname or "unknown"
    # Slugify the path: strip leading/trailing slashes, replace slashes with dashes
    path_slug = parsed.path.strip("/").replace("/", "-") or hostname
    # Keep only alphanumeric and dashes, collapse multiple dashes
    path_slug = re.sub(r"[^a-z0-9\-]+", "-", path_slug.lower())
    path_slug = re.sub(r"-+", "-", path_slug)
    # Truncate to a reasonable length
    if len(path_slug) > 100:
        path_slug = path_slug[:100]

    filename = f"{hostname}_{path_slug}.md"
    return output_dir / filename


def ensure_output_dir(output_dir: Path) -> Path:
    """Create the output directory if it doesn't exist."""
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


# ---------------------------------------------------------------------------
# .md endpoint helpers
# ---------------------------------------------------------------------------


def try_md_url(url: str) -> str | None:
    """Try fetching the .md variant of the URL and return raw text, or None.

    Many dev-doc sites (e.g. Foursquare) client-side render their docs.
    Appending ``.md`` to the path returns the raw markdown directly.
    """
    parsed = urlparse(url)
    # Insert .md before the last path segment, or at the end
    path = parsed.path
    if not path:
        return None
    path = path.rstrip("/")
    base, ext = path.rsplit(".", 1) if "." in path.rsplit("/", 1)[-1] else (path, "")
    if base == path:  # no extension
        new_path = path + ".md"
    else:
        new_path = path  # already has an extension
    md_url = f"{parsed.scheme}://{parsed.netloc}{new_path}"
    try:
        resp = requests.get(md_url, headers=HEADERS, timeout=30)
        if resp.status_code == 200 and len(resp.text) > 500:
            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
    except requests.RequestException:
        pass
    return None


def clean_md_endpoint(raw: str) -> str:
    """Strip a site's standard .md endpoint boilerplate.

    Many sites add frontmatter (---…---) and an "llms.txt" discovery line
    at the top of their .md endpoints.  This removes that noise so the
    actual content begins at the first ``#`` heading.
    """
    lines = raw.splitlines()
    start = 0

    # 1. Skip YAML frontmatter block
    if lines and lines[0].strip() == "---":
        for i, line in enumerate(lines, 1):
            if line.strip() == "---":
                start = i + 1
                break

    # 2. Skip leading lines that aren't a real heading (e.g. llms.txt, updatedAt)
    for i in range(start, len(lines)):
        stripped = lines[i].strip()
        if stripped.startswith("# "):
            start = i
            break

    return "\n".join(lines[start:])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

@click.command()
@click.argument("url")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to selectors YAML config (default: selectors.yaml next to this script)",
)
@click.option(
    "--output",
    "output_dir",
    type=click.Path(),
    default=None,
    help="Output directory for Markdown files (default: ./docs)",
)
def main(url: str, config_path: str | None, output_dir: str | None) -> None:
    """Extract main content from a developer documentation URL and save as Markdown.

    \b
    Examples:
        uv run docs_extractor.py "https://docs.foursquare.com/..."
        uv run docs_extractor.py "https://docs.foursquare.com/..." --config my-selectors.yaml
        uv run docs_extractor.py "https://docs.foursquare.com/..." --output ./my-docs
    """
    # Resolve config path
    if config_path:
        config_file = Path(config_path)
    else:
        config_file = DEFAULT_CONFIG

    # Resolve output directory
    if output_dir:
        out_dir = Path(output_dir)
    else:
        out_dir = DEFAULT_OUTPUT_DIR

    # Try .md URL first — many sites serve client-side rendered docs this way
    md_url_text = try_md_url(url)
    if md_url_text is not None:
        click.echo("Using .md endpoint for this URL", err=True)
        # Strip the site's standard .md header/footer boilerplate (frontmatter +
        # llms.txt instruction), then apply the normal cleaning pass.
        md = clean_md_endpoint(md_url_text)
        md = clean_markdown(md)
        out_dir = ensure_output_dir(out_dir)
        dest = output_path(url, out_dir)
        dest.write_text(md, encoding="utf-8")
        click.echo(f"Saved to: {dest}")
        return

    # Fall back to HTML + CSS selector extraction
    selectors = load_config(config_file)
    selector_cfg = find_selector(url, selectors)

    try:
        html = fetch_html(url)
    except requests.RequestException as e:
        click.echo(f"Error fetching URL: {e}", err=True)
        sys.exit(1)

    if selector_cfg:
        try:
            md = extract_html_to_md(html, selector_cfg.selector)
            click.echo(f"Using configured selector for '{selector_cfg.pattern}': "
                       f"{selector_cfg.selector}", err=True)
        except ValueError as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)
    else:
        click.echo(f"Error: no selector matched for '{url}'.", err=True)
        click.echo("Add an entry for this URL to selectors.yaml:", err=True)
        available = [s.pattern for s in selectors] if selectors else "none (config not found or empty)"
        click.echo(f"Available patterns: {available}", err=True)
        sys.exit(1)

    md = clean_markdown(md)

    # Write output
    out_dir = ensure_output_dir(out_dir)
    dest = output_path(url, out_dir)
    dest.write_text(md, encoding="utf-8")

    click.echo(f"Saved to: {dest}")


if __name__ == "__main__":
    main()
