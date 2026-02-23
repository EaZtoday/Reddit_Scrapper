"""
apify_adapter.py — Convert Apify Reddit scraper output to Reddit_Scrapper VoC format

Takes the JSON files from data/apify_results/ and converts them into the format
expected by the Reddit_Scrapper pipeline (db/writer.py insert_post schema).

Output: data/apify_adapted/posts.json — ready to import into the SQLite DB
        or feed directly into the GPT filter/insight pipeline.

Run standalone:
    python scripts/apify_adapter.py

Or after apify_scrape.py in the GitHub Action workflow.
"""

import json
import hashlib
import os
import sys
from datetime import datetime, UTC
from pathlib import Path

INPUT_DIR = Path("data/apify_results")
OUTPUT_DIR = Path("data/apify_adapted")


def parse_apify_date(date_str: str) -> float:
    """Convert Apify date string to Unix timestamp."""
    if not date_str:
        return datetime.now(UTC).timestamp()
    try:
        # Apify uses ISO 8601 format
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.timestamp()
    except (ValueError, AttributeError):
        return datetime.now(UTC).timestamp()


def generate_stable_id(item: dict) -> str:
    """Generate a stable ID from Apify item data for dedup."""
    # Prefer Reddit's native ID if available
    reddit_id = item.get("id") or item.get("postId")
    if reddit_id:
        return str(reddit_id)

    # Fallback: hash URL or title+body
    unique_str = item.get("url", "") or f"{item.get('title', '')}{item.get('body', '')}"
    return hashlib.sha256(unique_str.encode()).hexdigest()[:16]


def extract_subreddit(item: dict) -> str:
    """Extract subreddit name from Apify item."""
    # Direct field
    sub = item.get("subreddit") or item.get("communityName") or ""
    if sub:
        return sub.replace("r/", "").strip()

    # Extract from URL
    url = item.get("url", "")
    if "/r/" in url:
        parts = url.split("/r/")
        if len(parts) > 1:
            return parts[1].split("/")[0]

    return "unknown"


def adapt_post(item: dict) -> dict:
    """Convert a single Apify result item to Reddit_Scrapper post format."""
    # Determine if this is a post or comment
    is_comment = bool(item.get("parentId") or item.get("isComment"))
    item_type = "comment" if is_comment else "post"

    # Build URL
    url = item.get("url", "")
    if not url and item.get("permalink"):
        url = f"https://www.reddit.com{item['permalink']}"

    # Extract body text
    body = item.get("body") or item.get("text") or item.get("selftext") or ""

    # For comments, capture the parent post body if available
    post_body = ""
    parent_post_id = None
    if is_comment:
        post_body = item.get("postBody") or item.get("parentBody") or ""
        parent_post_id = item.get("parentId") or item.get("postId")

    created_utc = parse_apify_date(
        item.get("createdAt") or item.get("created_utc") or item.get("date")
    )

    return {
        "id": generate_stable_id(item),
        "title": item.get("title") or item.get("postTitle") or "",
        "body": body,
        "created_utc": created_utc,
        "subreddit": extract_subreddit(item),
        "url": url,
        "type": item_type,
        "post_body": post_body,
        "parent_post_id": parent_post_id,
        # Metadata from Apify (not in DB schema but useful for downstream)
        "_tier": item.get("_tier", "unknown"),
        "_search_term": item.get("_search_term", ""),
        "_score": item.get("score") or item.get("upvotes", 0),
        "_num_comments": item.get("numberOfComments") or item.get("numComments", 0),
    }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not INPUT_DIR.exists():
        print(f"No input directory found at {INPUT_DIR}")
        print("Run apify_scrape.py first to generate Apify results.")
        sys.exit(1)

    all_adapted = []
    seen_ids = set()

    # Process all JSON files in the results directory
    json_files = sorted(INPUT_DIR.glob("*.json"))
    if not json_files:
        print(f"No JSON files found in {INPUT_DIR}")
        sys.exit(1)

    for json_file in json_files:
        print(f"Processing {json_file.name}...")
        with open(json_file) as f:
            items = json.load(f)

        adapted = 0
        skipped = 0
        for item in items:
            post = adapt_post(item)

            # Dedup by ID
            if post["id"] in seen_ids:
                skipped += 1
                continue
            seen_ids.add(post["id"])

            # Skip items with no meaningful content
            if not post["title"] and not post["body"]:
                skipped += 1
                continue

            all_adapted.append(post)
            adapted += 1

        print(f"  -> {adapted} adapted, {skipped} skipped (dupes/empty)")

    # Save adapted posts
    output_file = OUTPUT_DIR / "posts.json"
    with open(output_file, "w") as f:
        json.dump(all_adapted, f, indent=2, default=str)

    # Also save a summary with tier breakdown
    tier_counts = {}
    for post in all_adapted:
        tier = post.get("_tier", "unknown")
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

    summary = {
        "total_posts": len(all_adapted),
        "unique_subreddits": len(set(p["subreddit"] for p in all_adapted)),
        "tier_breakdown": tier_counts,
        "generated_at": datetime.now(UTC).isoformat(),
    }

    summary_file = OUTPUT_DIR / "summary.json"
    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n{'='*50}")
    print(f"  ADAPTED: {len(all_adapted)} posts")
    print(f"  Subreddits: {summary['unique_subreddits']}")
    print(f"  Tier breakdown: {tier_counts}")
    print(f"  Output: {output_file}")
    print(f"  Summary: {summary_file}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
