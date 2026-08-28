#!/usr/bin/env python3
"""Extract configured developer-documentation content as agent-readable Markdown."""

import argparse
import json
import re
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup
from markdownify import markdownify
from soupsieve import SelectorSyntaxError


DEFAULT_SELECTOR_FILE = Path(__file__).with_name("doc_content_selectors.json")
USER_AGENT = "utilities-doc-to-markdown/1.0"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download developer documentation and extract its main content as Markdown.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("urls", nargs="+", help="Documentation URL(s) to convert.")
    parser.add_argument(
        "-o",
        "--output",
        help=(
            "Output Markdown file for one URL, or output directory for multiple URLs. "
            "Prints to stdout when omitted."
        ),
    )
    parser.add_argument(
        "--selector",
        help="CSS selector for the main content; overrides configured selectors.",
    )
    parser.add_argument(
        "--selectors-file",
        type=Path,
        default=DEFAULT_SELECTOR_FILE,
        help="JSON file containing URL-prefix-to-CSS-selector mappings.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30,
        help="HTTP request timeout in seconds.",
    )
    return parser.parse_args()


def load_selector_mappings(path):
    try:
        mappings = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ValueError(f"Selector mapping file does not exist: {path}")
    except json.JSONDecodeError as error:
        raise ValueError(f"Invalid JSON in selector mapping file {path}: {error}") from error

    if not isinstance(mappings, dict):
        raise ValueError("Selector mappings must be a JSON object of URL prefixes to CSS selectors.")

    invalid_mappings = [
        prefix
        for prefix, selector in mappings.items()
        if not isinstance(prefix, str) or not isinstance(selector, str) or not selector.strip()
    ]
    if invalid_mappings:
        raise ValueError("Each selector mapping must have a string URL prefix and CSS selector.")
    return mappings


def selector_for_url(url, mappings, explicit_selector=None):
    if explicit_selector:
        return explicit_selector

    matching_prefixes = [prefix for prefix in mappings if url.startswith(prefix)]
    if not matching_prefixes:
        return None
    return mappings[max(matching_prefixes, key=len)]


def fetch_html(url, timeout):
    request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "text/html,*/*"})
    try:
        with urlopen(request, timeout=timeout) as response:
            content_type = response.headers.get_content_type()
            if content_type not in {"text/html", "application/xhtml+xml"}:
                raise ValueError(f"Expected HTML, received {content_type}.")
            return response.read()
    except HTTPError as error:
        raise ValueError(f"Request failed with HTTP {error.code}: {error.reason}") from error
    except URLError as error:
        raise ValueError(f"Could not fetch URL: {error.reason}") from error


def absolutize_urls(content, page_url):
    for element in content.find_all(["a", "img"]):
        attribute = "href" if element.name == "a" else "src"
        value = element.get(attribute)
        if value:
            element[attribute] = urljoin(page_url, value)


def page_title(soup):
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    return title or "Documentation"


def code_language(element):
    code = element.find("code")
    classes = (code or element).get("class", [])
    for class_name in classes:
        if class_name.startswith("language-"):
            return class_name.removeprefix("language-")
        if class_name.startswith("lang-"):
            return class_name.removeprefix("lang-")
    return None


def convert_html(url, html, selector):
    soup = BeautifulSoup(html, "html.parser")
    try:
        content = (
            soup.select_one(selector)
            if selector
            else (
                soup.find("main")
                or soup.select_one("[role='main']")
                or soup.find("article")
                or soup.body
            )
        )
    except SelectorSyntaxError as error:
        raise ValueError(f"Invalid CSS selector {selector!r}: {error}") from error
    if content is None:
        location = selector or "a <main> or <body> element"
        raise ValueError(f"Could not find main content using {location}.")

    for element in content.select("script, style, noscript, template"):
        element.decompose()
    absolutize_urls(content, url)

    markdown = markdownify(
        str(content),
        heading_style="ATX",
        bullets="-",
        code_language_callback=code_language,
    ).strip()
    if not markdown:
        raise ValueError("The selected main content is empty after conversion.")

    if markdown.startswith("# "):
        return f"Source: {url}\n\n{markdown}\n"
    return f"# {page_title(soup)}\n\nSource: {url}\n\n{markdown}\n"


def output_path_for_url(url, output):
    parsed = urlparse(url)
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", parsed.path.strip("/")) or "index"
    return output / f"{stem}.md"


def write_output(url, markdown, output, multiple_urls):
    if output is None:
        print(markdown, end="")
        return

    path = Path(output)
    if multiple_urls:
        path.mkdir(parents=True, exist_ok=True)
        path = output_path_for_url(url, path)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(markdown, encoding="utf-8")
    print(f"Wrote {path}")


def main():
    args = parse_args()
    if args.timeout <= 0:
        print("--timeout must be greater than zero.", file=sys.stderr)
        return 2
    if len(args.urls) > 1 and args.output and Path(args.output).suffix:
        print("--output must be a directory when converting multiple URLs.", file=sys.stderr)
        return 2

    try:
        mappings = load_selector_mappings(args.selectors_file)
    except ValueError as error:
        print(f"Error: {error}", file=sys.stderr)
        return 2

    exit_code = 0
    for url in args.urls:
        selector = selector_for_url(url, mappings, args.selector)
        try:
            markdown = convert_html(url, fetch_html(url, args.timeout), selector)
            write_output(url, markdown, args.output, len(args.urls) > 1)
        except ValueError as error:
            print(f"Error converting {url}: {error}", file=sys.stderr)
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
