from __future__ import annotations

from datetime import date as date_type
from datetime import datetime, timezone
from html import unescape
import json
from pathlib import Path
import re
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import yaml


ROOT = Path(__file__).resolve().parent.parent
CONFIG_FILE = ROOT / "_config.yml"
POSTS_DIR = ROOT / "_posts"
OUTPUT_FILE = ROOT / "_data" / "webmentions.yml"
TOP_POSTS_FILE = ROOT / "_data" / "top_posts.yml"
OVERRIDES_FILE = ROOT / "_data" / "webmention_overrides.yml"
COUNT_API = "https://webmention.io/api/count"
MENTIONS_API = "https://webmention.io/api/mentions.jf2"
USER_AGENT = "nuchronic-webmention-sync/1.0"
TOP_POST_LIMIT = 30
SLUG_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}-(.+)$")
TAG_PATTERN = re.compile(r"<[^>]+>")


def load_site_url() -> str:
    payload = yaml.safe_load(CONFIG_FILE.read_text(encoding="utf-8")) or {}
    site_url = str(payload.get("url", "")).strip().rstrip("/")
    if not site_url:
        raise ValueError("The Jekyll config must define a non-empty url.")
    return site_url


def derive_slug(post_path: Path) -> str:
    match = SLUG_PATTERN.match(post_path.stem)
    if match:
        return match.group(1)
    return post_path.stem


def read_front_matter(post_path: Path) -> dict[str, object]:
    text = post_path.read_text(encoding="utf-8")
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}

    for index in range(1, len(lines)):
        if lines[index].strip() == "---":
            payload = yaml.safe_load("\n".join(lines[1:index])) or {}
            return payload if isinstance(payload, dict) else {}

    return {}


def parse_post_timestamp(value: object) -> float:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date_type):
        parsed = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    else:
        raw_value = str(value or "").strip()
        parsed = None
        for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(raw_value, fmt)
                break
            except ValueError:
                continue

        if parsed is None:
            return 0.0

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).timestamp()


def fetch_json(base_url: str, params: dict[str, object]) -> dict[str, object]:
    url = f"{base_url}?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def strip_html(value: str) -> str:
    return unescape(TAG_PATTERN.sub("", value)).strip()


def extract_content_text(entry: dict[str, object]) -> str:
    content = entry.get("content")
    if isinstance(content, dict):
        text_value = str(content.get("text", "")).strip()
        if text_value:
            return text_value

        html_value = str(content.get("html", "")).strip()
        if html_value:
            return strip_html(html_value)

    if isinstance(content, str) and content.strip():
        return content.strip()

    summary = str(entry.get("summary", "")).strip()
    return strip_html(summary) if summary else ""


def normalize_author(entry: dict[str, object]) -> dict[str, str]:
    author = entry.get("author")
    if not isinstance(author, dict):
        author = {}

    author_url = str(author.get("url", "")).strip()
    author_name = str(author.get("name", "")).strip() or author_url or "Someone"
    author_photo = str(author.get("photo", "")).strip()
    return {
        "author_name": author_name,
        "author_url": author_url,
        "author_photo": author_photo,
    }


def classify_mention(property_name: str) -> str:
    if property_name == "in-reply-to":
        return "replies"
    if property_name in {"like-of", "favorite-of", "bookmark-of", "rsvp-yes", "rsvp-no", "rsvp-maybe", "rsvp-interested", "emoji-react-of"}:
        return "likes"
    if property_name in {"repost-of", "share-of"}:
        return "reposts"
    return "mentions"


def normalize_mention(entry: dict[str, object]) -> dict[str, str]:
    property_name = str(entry.get("wm-property", "mention-of")).strip().lower()
    normalized = normalize_author(entry)
    normalized.update(
        {
            "url": str(entry.get("url", "")).strip(),
            "published": str(entry.get("published", "")).strip() or str(entry.get("wm-received", "")).strip(),
            "property": property_name,
            "content_text": extract_content_text(entry),
        }
    )
    return normalized


def sort_mentions(items: list[dict[str, str]]) -> list[dict[str, str]]:
    return sorted(items, key=lambda item: (item.get("published", ""), item.get("author_name", "")), reverse=True)


def load_existing_data() -> dict[str, object]:
    if not OUTPUT_FILE.exists():
        return {}
    payload = yaml.safe_load(OUTPUT_FILE.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}


def load_override_data() -> dict[str, object]:
    if not OVERRIDES_FILE.exists():
        return {}
    payload = yaml.safe_load(OVERRIDES_FILE.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}


def build_default_record(target_url: str) -> dict[str, object]:
    return {
        "target": target_url,
        "count": 0,
        "types": {},
        "replies": [],
        "likes": [],
        "reposts": [],
        "mentions": [],
    }


def build_record(target_url: str) -> dict[str, object]:
    count_payload = fetch_json(COUNT_API, {"target": target_url})
    mentions_payload = fetch_json(MENTIONS_API, {"target": target_url, "per-page": 1000})

    replies: list[dict[str, str]] = []
    likes: list[dict[str, str]] = []
    reposts: list[dict[str, str]] = []
    mentions: list[dict[str, str]] = []

    for child in mentions_payload.get("children", []):
        if not isinstance(child, dict):
            continue

        normalized = normalize_mention(child)
        bucket = classify_mention(normalized["property"])
        if bucket == "replies":
            replies.append(normalized)
        elif bucket == "likes":
            likes.append(normalized)
        elif bucket == "reposts":
            reposts.append(normalized)
        else:
            mentions.append(normalized)

    record = build_default_record(target_url)
    record["count"] = int(count_payload.get("count", 0) or 0)
    raw_types = count_payload.get("type")
    if isinstance(raw_types, dict):
        record["types"] = {str(key): int(value or 0) for key, value in raw_types.items()}
    record["replies"] = sort_mentions(replies)
    record["likes"] = sort_mentions(likes)
    record["reposts"] = sort_mentions(reposts)
    record["mentions"] = sort_mentions(mentions)
    return record


def build_top_posts(post_records: list[dict[str, object]], mention_records: dict[str, object]) -> list[dict[str, object]]:
    ranked_posts: list[dict[str, object]] = []

    for post_record in post_records:
        slug = str(post_record["slug"])
        mention_record = mention_records.get(slug, {})
        count = 0
        if isinstance(mention_record, dict):
            count = int(mention_record.get("count", 0) or 0)

        ranked_posts.append(
            {
                "slug": slug,
                "count": count,
                "sort_timestamp": float(post_record.get("sort_timestamp", 0.0) or 0.0),
            }
        )

    ranked_posts.sort(key=lambda item: (-int(item["count"]), -float(item["sort_timestamp"]), str(item["slug"])))
    return [{"slug": item["slug"], "count": item["count"]} for item in ranked_posts[:TOP_POST_LIMIT]]


def write_output(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = yaml.safe_dump(payload, sort_keys=False, allow_unicode=True).strip()
    path.write_text(f"{content}\n", encoding="utf-8")


def main() -> int:
    site_url = load_site_url()
    existing_data = load_existing_data()
    override_data = load_override_data()
    results: dict[str, object] = {}
    post_records: list[dict[str, object]] = []

    for post_path in sorted(POSTS_DIR.glob("*.md")):
        slug = derive_slug(post_path)
        target_url = f"{site_url}/item/{slug}/"
        front_matter = read_front_matter(post_path)
        post_records.append(
            {
                "slug": slug,
                "sort_timestamp": parse_post_timestamp(front_matter.get("date")),
            }
        )

        try:
            results[slug] = build_record(target_url)
        except Exception as exc:
            if slug in existing_data:
                print(f"Warning: failed to update {slug}: {exc}. Keeping existing data.")
                results[slug] = existing_data[slug]
            else:
                print(f"Warning: failed to update {slug}: {exc}. Falling back to an empty record.")
                results[slug] = build_default_record(target_url)

        override_record = override_data.get(slug)
        if isinstance(override_record, dict):
            results[slug] = override_record

    top_posts = build_top_posts(post_records, results)
    write_output(OUTPUT_FILE, results)
    write_output(TOP_POSTS_FILE, top_posts)
    print(f"Webmention sync complete: wrote {len(results)} record(s).")
    return 0



if __name__ == "__main__":
    raise SystemExit(main())