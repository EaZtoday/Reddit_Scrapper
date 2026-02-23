"""
apify_import.py — Import Apify-adapted posts into the Reddit_Scrapper SQLite database

Reads data/apify_adapted/posts.json and inserts into the posts table,
making them available for the GPT filter and insight extraction pipeline.

Run after apify_adapter.py:
    python scripts/apify_import.py

This bridges the gap between Apify scraping and the existing VoC pipeline.
"""

import json
import sys
from pathlib import Path

# Add project root to path so we can import from db/
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.schema import create_tables
from db.writer import insert_post
from db.reader import is_already_processed
from utils.helpers import ensure_directory_exists

ADAPTED_FILE = Path("data/apify_adapted/posts.json")


def main():
    if not ADAPTED_FILE.exists():
        print(f"No adapted file found at {ADAPTED_FILE}")
        print("Run apify_adapter.py first.")
        sys.exit(1)

    # Initialize database
    ensure_directory_exists("data")
    create_tables()

    with open(ADAPTED_FILE) as f:
        posts = json.load(f)

    inserted = 0
    skipped = 0

    for post in posts:
        if is_already_processed(post["id"]):
            skipped += 1
            continue

        # Determine community type based on tier
        tier = post.get("_tier", "")
        if "icp_specific" in tier:
            community_type = "primary"
        elif "competitor" in tier:
            community_type = "exploratory"
        else:
            community_type = "primary"

        # Strip adapter metadata before inserting
        clean_post = {
            "id": post["id"],
            "title": post["title"],
            "body": post["body"],
            "created_utc": post["created_utc"],
            "subreddit": post["subreddit"],
            "url": post["url"],
            "type": post["type"],
            "post_body": post.get("post_body", ""),
            "parent_post_id": post.get("parent_post_id"),
        }

        insert_post(clean_post, community_type=community_type)
        inserted += 1

    print(f"Import complete: {inserted} inserted, {skipped} skipped (already processed)")
    print(f"Posts are now in the database and ready for the GPT filter/insight pipeline.")
    print(f"Run `python main.py` to process them through the full VoC extraction.")


if __name__ == "__main__":
    main()
