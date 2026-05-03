#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urldefrag, urljoin, urlsplit, urlunsplit
from urllib.request import Request, urlopen

import yaml


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "_python"))

from fetch_webmentions import derive_slug, load_site_url, read_front_matter  # noqa: E402


CACHE_DIR = ROOT / ".cache"
BRIDGY_CACHE_FILE = CACHE_DIR / "bridgy_publish.yml"
OUTGOING_WEBMENTIONS_FILE = CACHE_DIR / "outgoing_webmentions.yml"
LOOKUPS_FILE = CACHE_DIR / "webmention_lookups.yml"
BAD_URIS_FILE = CACHE_DIR / "webmention_bad_uris.yml"
SYNDICATION_FILE = ROOT / "_data" / "syndication_links.yml"
POSTS_DIR = ROOT / "_posts"

BRIDGY_ENDPOINT = "https://brid.gy/publish/webmention"
BRIDGY_TARGETS = ("bluesky", "mastodon")
USER_AGENT = "nuchronic-outgoing/1.0"
MAX_HTML_BYTES = 512 * 1024
LINK_HEADER_SPLIT_RE = re.compile(r",\s*(?=<)")


class EndpointParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.endpoint: str = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self.endpoint:
            return

        if tag not in {"a", "link"}:
            return

        attr_map = {name.lower(): (value or "") for name, value in attrs}
        rel_value = attr_map.get("rel", "")
        rel_tokens = {token.strip().lower() for token in rel_value.split() if token.strip()}
        if "webmention" not in rel_tokens:
            return

        href = attr_map.get("href", "").strip()
        if href:
            self.endpoint = href


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def clean_url(value: str) -> str:
    raw_value = urldefrag(str(value or "").strip())[0]
    if not raw_value:
        return ""

    parsed = urlsplit(raw_value)
    query_pairs = [(key, item) for key, item in parse_qsl(parsed.query, keep_blank_values=True) if not key.lower().startswith("utm_")]
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query_pairs, doseq=True), ""))


def excerpt(value: str, limit: int = 280) -> str:
    collapsed = " ".join(str(value or "").split())
    return collapsed[:limit]


def load_yaml_map(path: Path, root_key: str) -> dict[str, Any]:
    payload: dict[str, Any]
    if path.exists():
        parsed = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        payload = parsed if isinstance(parsed, dict) else {}
    else:
        payload = {}

    payload.setdefault("version", 1)
    payload.setdefault(root_key, {})
    if not isinstance(payload[root_key], dict):
        payload[root_key] = {}
    return payload


def write_yaml(path: Path, payload: object) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = yaml.safe_dump(payload, sort_keys=False, allow_unicode=True).strip() + "\n"
    current = path.read_text(encoding="utf-8") if path.exists() else None
    if current == content:
        return False
    path.write_text(content, encoding="utf-8")
    return True


def run_git(*args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def list_new_post_paths(before_sha: str, current_sha: str) -> list[Path]:
    if before_sha == "0" * 40:
        return []

    output = run_git("diff", "--diff-filter=A", "--name-only", before_sha, current_sha, "--", "_posts/")
    paths: list[Path] = []
    for line in output.splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        path = ROOT / candidate
        if path.exists() and path.is_file():
            paths.append(path)
    return paths


def parse_link_headers(header_values: list[str], base_url: str) -> str:
    for value in header_values:
        for part in LINK_HEADER_SPLIT_RE.split(value):
            url_match = re.search(r"<([^>]+)>", part)
            rel_match = re.search(r";\s*rel=(?:\"([^\"]+)\"|([^;]+))", part, flags=re.IGNORECASE)
            if not url_match or not rel_match:
                continue

            rel_value = (rel_match.group(1) or rel_match.group(2) or "").strip()
            rel_tokens = {token.strip().lower() for token in rel_value.split() if token.strip()}
            if "webmention" in rel_tokens:
                return urljoin(base_url, url_match.group(1).strip())
    return ""


def request_url(url: str, *, data: bytes | None = None, accept: str = "*/*", timeout: int = 30) -> dict[str, Any]:
    request = Request(
        url,
        data=data,
        headers={
            "Accept": accept,
            "User-Agent": USER_AGENT,
        },
    )

    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read(MAX_HTML_BYTES if data is None else 1024 * 1024)
            return {
                "status": getattr(response, "status", response.getcode()),
                "url": response.geturl(),
                "headers": response.headers,
                "body": body,
            }
    except HTTPError as error:
        body = error.read(1024 * 1024)
        return {
            "status": error.code,
            "url": error.geturl(),
            "headers": error.headers,
            "body": body,
        }
    except URLError as error:
        raise RuntimeError(str(error.reason)) from error


def decode_body(response: dict[str, Any]) -> str:
    headers = response["headers"]
    charset = headers.get_content_charset() if hasattr(headers, "get_content_charset") else None
    return response["body"].decode(charset or "utf-8", errors="replace")


def discover_webmention_endpoint(
    target_url: str,
    lookups_cache: dict[str, Any],
    bad_uris_cache: dict[str, Any],
) -> tuple[str, str, str]:
    target_key = clean_url(target_url)
    lookup_entry = lookups_cache["targets"].get(target_key)
    if isinstance(lookup_entry, dict):
        endpoint = str(lookup_entry.get("endpoint", "")).strip()
        resolved_url = clean_url(str(lookup_entry.get("resolved_url", target_key)).strip()) or target_key
        if endpoint:
            return resolved_url, endpoint, "cached"

    bad_entry = bad_uris_cache["targets"].get(target_key)
    if isinstance(bad_entry, dict):
        resolved_url = clean_url(str(bad_entry.get("resolved_url", target_key)).strip()) or target_key
        return resolved_url, "", str(bad_entry.get("reason", "cached-bad-uri")).strip() or "cached-bad-uri"

    response = request_url(target_key, accept="text/html,application/xhtml+xml,*/*;q=0.8")
    final_url = clean_url(response["url"]) or target_key
    if response["status"] >= 400:
        bad_uris_cache["targets"][target_key] = {
            "resolved_url": final_url,
            "reason": f"discovery-http-{response['status']}",
            "checked_at": now_iso(),
        }
        return final_url, "", f"discovery-http-{response['status']}"

    header_values = response["headers"].get_all("Link", []) if hasattr(response["headers"], "get_all") else []
    endpoint = parse_link_headers(header_values, final_url)
    body_text = decode_body(response)
    if not endpoint:
        parser = EndpointParser()
        parser.feed(body_text)
        endpoint = urljoin(final_url, parser.endpoint) if parser.endpoint else ""

    lookup_record = {
        "resolved_url": final_url,
        "endpoint": endpoint,
        "checked_at": now_iso(),
    }
    lookups_cache["targets"][target_key] = lookup_record

    if endpoint:
        bad_uris_cache["targets"].pop(target_key, None)
        return final_url, endpoint, "discovered"

    bad_uris_cache["targets"][target_key] = {
        "resolved_url": final_url,
        "reason": "no-endpoint",
        "checked_at": now_iso(),
    }
    return final_url, "", "no-endpoint"


def post_form(endpoint: str, payload: dict[str, str]) -> dict[str, Any]:
    return request_url(endpoint, data=urlencode(payload).encode("utf-8"), accept="*/*")


def ensure_post_entry(cache: dict[str, Any], slug: str, source_url: str, container_key: str) -> dict[str, Any]:
    posts = cache["posts"]
    entry = posts.get(slug)
    if not isinstance(entry, dict):
        entry = {
            "source_url": source_url,
            container_key: {},
        }
        posts[slug] = entry

    entry["source_url"] = source_url
    if not isinstance(entry.get(container_key), dict):
        entry[container_key] = {}
    return entry


def publish_to_bridgy(slug: str, source_url: str, bridgy_cache: dict[str, Any], *, dry_run: bool) -> tuple[bool, int]:
    post_entry = ensure_post_entry(bridgy_cache, slug, source_url, "networks")
    failures = 0
    changes = False

    for network in BRIDGY_TARGETS:
        existing = post_entry["networks"].get(network)
        if isinstance(existing, dict) and existing.get("status") == "success" and existing.get("url"):
            continue

        if dry_run:
            print(f"Would publish {source_url} to {network} via Bridgy")
            continue

        target_url = f"https://brid.gy/publish/{network}"
        response = post_form(BRIDGY_ENDPOINT, {"source": source_url, "target": target_url})
        body_text = decode_body(response)

        payload: dict[str, Any] = {}
        if body_text:
            try:
                parsed = json.loads(body_text)
                if isinstance(parsed, dict):
                    payload = parsed
            except json.JSONDecodeError:
                payload = {}

        record = {
            "status": "success" if 200 <= response["status"] < 300 else "failed",
            "target": target_url,
            "response_status": int(response["status"]),
            "updated_at": now_iso(),
        }
        publish_url = str(payload.get("url", "")).strip() or str(response["headers"].get("Location", "")).strip()
        publish_id = str(payload.get("id", "")).strip()
        granary_message = str(payload.get("granary_message", "")).strip()
        if publish_url:
            record["url"] = publish_url
        if publish_id:
            record["id"] = publish_id
        if granary_message:
            record["granary_message"] = granary_message

        if record["status"] == "success":
            record["sent_at"] = now_iso()
            print(f"Published {source_url} to {network}: {publish_url or publish_id or response['status']}")
        else:
            failures += 1
            record["error"] = excerpt(body_text) or f"HTTP {response['status']}"
            print(f"Failed to publish {source_url} to {network}: {record['error']}")

        post_entry["networks"][network] = record
        changes = True

    return changes, failures


def send_outgoing_webmention(
    slug: str,
    source_url: str,
    target_url: str,
    outgoing_cache: dict[str, Any],
    lookups_cache: dict[str, Any],
    bad_uris_cache: dict[str, Any],
    *,
    dry_run: bool,
) -> tuple[bool, int]:
    cleaned_target = clean_url(target_url)
    if not cleaned_target:
        return False, 0

    post_entry = ensure_post_entry(outgoing_cache, slug, source_url, "targets")
    existing = post_entry["targets"].get(cleaned_target)
    if isinstance(existing, dict) and existing.get("status") == "success":
        return False, 0

    if dry_run:
        print(f"Would send webmention from {source_url} to {cleaned_target}")
        return False, 0

    resolved_url, endpoint, discovery_status = discover_webmention_endpoint(cleaned_target, lookups_cache, bad_uris_cache)
    record: dict[str, Any] = {
        "target_url": cleaned_target,
        "resolved_url": resolved_url,
        "updated_at": now_iso(),
    }

    if not endpoint:
        record["status"] = "skipped"
        record["reason"] = discovery_status
        post_entry["targets"][cleaned_target] = record
        print(f"Skipped webmention for {cleaned_target}: {discovery_status}")
        return True, 0

    record["endpoint"] = endpoint
    response = post_form(endpoint, {"source": source_url, "target": resolved_url})
    body_text = decode_body(response)
    if 200 <= response["status"] < 300:
        record["status"] = "success"
        record["sent_at"] = now_iso()
        record["response_status"] = int(response["status"])
        post_entry["targets"][cleaned_target] = record
        print(f"Sent webmention {source_url} -> {resolved_url}")
        return True, 0

    record["status"] = "failed"
    record["response_status"] = int(response["status"])
    record["error"] = excerpt(body_text) or f"HTTP {response['status']}"
    post_entry["targets"][cleaned_target] = record
    print(f"Failed webmention {source_url} -> {resolved_url}: {record['error']}")
    return True, 1


def build_public_syndication_data(bridgy_cache: dict[str, Any]) -> dict[str, Any]:
    public_data: dict[str, Any] = {}
    posts = bridgy_cache.get("posts", {})
    if not isinstance(posts, dict):
        return public_data

    for slug, post_entry in posts.items():
        if not isinstance(post_entry, dict):
            continue

        networks = post_entry.get("networks", {})
        if not isinstance(networks, dict):
            continue

        public_entry: dict[str, Any] = {}
        for network in BRIDGY_TARGETS:
            details = networks.get(network)
            if not isinstance(details, dict):
                continue
            if details.get("status") != "success" or not details.get("url"):
                continue

            network_entry: dict[str, Any] = {"url": str(details["url"]).strip()}
            if details.get("id"):
                network_entry["id"] = str(details["id"]).strip()
            public_entry[network] = network_entry

        if public_entry:
            public_data[str(slug)] = public_entry

    return public_data


def process_posts(before_sha: str, current_sha: str, *, dry_run: bool) -> int:
    site_url = load_site_url()
    new_posts = list_new_post_paths(before_sha, current_sha)
    if not new_posts:
        print("No new posts detected, skipping publication.")
        return 0

    bridgy_cache = load_yaml_map(BRIDGY_CACHE_FILE, "posts")
    outgoing_cache = load_yaml_map(OUTGOING_WEBMENTIONS_FILE, "posts")
    lookups_cache = load_yaml_map(LOOKUPS_FILE, "targets")
    bad_uris_cache = load_yaml_map(BAD_URIS_FILE, "targets")

    any_changes = False
    failures = 0

    for post_path in new_posts:
        slug = derive_slug(post_path)
        source_url = f"{site_url}/item/{slug}/"
        front_matter = read_front_matter(post_path)
        source_link = clean_url(str(front_matter.get("link", "")).strip())

        changed, publish_failures = publish_to_bridgy(slug, source_url, bridgy_cache, dry_run=dry_run)
        any_changes = any_changes or changed
        failures += publish_failures

        if source_link:
            changed, webmention_failures = send_outgoing_webmention(
                slug,
                source_url,
                source_link,
                outgoing_cache,
                lookups_cache,
                bad_uris_cache,
                dry_run=dry_run,
            )
            any_changes = any_changes or changed
            failures += webmention_failures

    if dry_run:
        print(f"Dry run complete for {len(new_posts)} new post(s).")
        return 0

    public_data = build_public_syndication_data(bridgy_cache)

    wrote_any = False
    wrote_any = write_yaml(BRIDGY_CACHE_FILE, bridgy_cache) or wrote_any
    wrote_any = write_yaml(OUTGOING_WEBMENTIONS_FILE, outgoing_cache) or wrote_any
    wrote_any = write_yaml(LOOKUPS_FILE, lookups_cache) or wrote_any
    wrote_any = write_yaml(BAD_URIS_FILE, bad_uris_cache) or wrote_any
    wrote_any = write_yaml(SYNDICATION_FILE, public_data) or wrote_any

    if wrote_any or any_changes:
        print(f"Processed {len(new_posts)} new post(s).")
    else:
        print("No publication state changed.")

    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish newly added posts to Bridgy and send outgoing source webmentions.")
    parser.add_argument("before_sha")
    parser.add_argument("current_sha")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    return process_posts(args.before_sha, args.current_sha, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())