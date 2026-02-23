"""
noauth_scrape.py — Zero-credential Reddit scraper for Zen Books VoC pipeline

Uses Reddit's public JSON endpoints (no API key, no OAuth, no Apify token).
Same 4-tier search strategy as apify_scrape.py, outputs to data/apify_results/
so the existing adapter → import → pipeline flow works unchanged.

Strategy:
  1. Primary: old.reddit.com JSON (more permissive than www)
  2. Fallback: PullPush.io API (Reddit archive, no auth needed)

Usage:
    python scripts/noauth_scrape.py                    # all tiers
    python scripts/noauth_scrape.py --tier 1_pain_language
    python scripts/noauth_scrape.py --tier 3_icp_specific --max-items 50
    python scripts/noauth_scrape.py --time-filter week --max-items 25
"""

import argparse
import json
import random
import time
import sys
from datetime import datetime, timezone, timedelta, UTC
from pathlib import Path
from urllib.parse import quote_plus

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

OUTPUT_DIR = Path("data/apify_results")
REQUEST_DELAY = 3.0  # seconds between requests (respect rate limits)
MAX_RETRIES = 3

# Browser-realistic headers — Reddit blocks obvious bots
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

# Time filter mapping for PullPush API (uses epoch timestamps)
TIME_FILTER_DAYS = {
    "hour": 1/24,
    "day": 1,
    "week": 7,
    "month": 30,
    "year": 365,
    "all": 3650,
}

# ---------------------------------------------------------------------------
# 4-Tier search queries — identical to apify_scrape.py
# ---------------------------------------------------------------------------

TIER_1_SEARCHES = [
    {"term": "bookkeeping mess help",           "subreddits": ["smallbusiness", "Entrepreneur"]},
    {"term": "QuickBooks disaster",             "subreddits": ["QuickBooks", "Bookkeeping"]},
    {"term": "years behind on books",           "subreddits": ["smallbusiness", "Bookkeeping", "tax"]},
    {"term": "shoebox of receipts",             "subreddits": ["smallbusiness", "Entrepreneur"]},
    {"term": "accountant fired me",             "subreddits": ["smallbusiness", "accounting", "tax"]},
    {"term": "bookkeeping overwhelmed",         "subreddits": ["smallbusiness", "Entrepreneur"]},
    {"term": "don't know if I'm profitable",    "subreddits": ["smallbusiness", "Entrepreneur"]},
    {"term": "tax time nightmare",              "subreddits": ["smallbusiness", "tax"]},
    {"term": "uncategorized transactions",      "subreddits": ["QuickBooks", "Bookkeeping"]},
    {"term": "CPA asking for reports",          "subreddits": ["smallbusiness", "tax", "accounting"]},
]

TIER_2_SEARCHES = [
    {"term": "hire a bookkeeper",               "subreddits": ["smallbusiness", "Entrepreneur"]},
    {"term": "need bookkeeping help",           "subreddits": ["smallbusiness", "Bookkeeping"]},
    {"term": "looking for a bookkeeper",        "subreddits": ["smallbusiness", "Entrepreneur"]},
    {"term": "QuickBooks setup help",           "subreddits": ["QuickBooks", "smallbusiness"]},
    {"term": "bookkeeping service recommendation", "subreddits": ["smallbusiness", "Entrepreneur"]},
    {"term": "how much does a bookkeeper cost", "subreddits": ["smallbusiness", "Bookkeeping"]},
    {"term": "virtual bookkeeper",              "subreddits": ["smallbusiness", "Entrepreneur"]},
    {"term": "bookkeeper vs accountant",        "subreddits": ["smallbusiness", "accounting"]},
    {"term": "catch up bookkeeping",            "subreddits": ["smallbusiness", "Bookkeeping", "QuickBooks"]},
    {"term": "clean up my books",               "subreddits": ["smallbusiness", "QuickBooks"]},
]

TIER_3_SEARCHES = [
    # Real Estate Investors
    {"term": "rental property bookkeeping",     "subreddits": ["realestateinvesting", "landlord", "CommercialRealEstate"]},
    {"term": "LLC QuickBooks multiple entities", "subreddits": ["realestateinvesting", "landlord"]},
    {"term": "property manager accounting",     "subreddits": ["realestateinvesting", "landlord"]},
    {"term": "lender financial statements rental", "subreddits": ["realestateinvesting", "CommercialRealEstate"]},
    {"term": "CapEx repairs bookkeeping",       "subreddits": ["realestateinvesting", "landlord"]},

    # Therapists & Solo Consultants
    {"term": "therapist bookkeeping",           "subreddits": ["therapists", "privatepractice"]},
    {"term": "private practice accounting",     "subreddits": ["therapists", "privatepractice"]},
    {"term": "SimplePractice QuickBooks",       "subreddits": ["therapists", "privatepractice"]},
    {"term": "solo practice business finances", "subreddits": ["therapists", "privatepractice"]},
    {"term": "therapist tax deductions",        "subreddits": ["therapists", "privatepractice", "tax"]},

    # Nonprofits
    {"term": "nonprofit bookkeeping",           "subreddits": ["nonprofit"]},
    {"term": "restricted fund tracking",        "subreddits": ["nonprofit"]},
    {"term": "nonprofit board financial report", "subreddits": ["nonprofit"]},
    {"term": "grant compliance accounting",     "subreddits": ["nonprofit"]},
    {"term": "nonprofit audit preparation",     "subreddits": ["nonprofit"]},

    # Restaurants & Construction
    {"term": "restaurant bookkeeping",          "subreddits": ["restaurateur"]},
    {"term": "construction job costing",        "subreddits": ["Construction"]},
    {"term": "contractor 1099 bookkeeping",     "subreddits": ["Construction", "smallbusiness"]},
    {"term": "restaurant payroll tips accounting", "subreddits": ["restaurateur"]},
    {"term": "subcontractor tracking QuickBooks", "subreddits": ["Construction"]},
]

TIER_4_SEARCHES = [
    {"term": "Bench bookkeeping review",        "subreddits": ["smallbusiness", "Bookkeeping"]},
    {"term": "bookkeeper made things worse",    "subreddits": ["smallbusiness", "Bookkeeping"]},
    {"term": "fired my bookkeeper",             "subreddits": ["smallbusiness", "Bookkeeping"]},
    {"term": "QuickBooks Live review",          "subreddits": ["QuickBooks", "smallbusiness"]},
    {"term": "offshore bookkeeper problems",    "subreddits": ["smallbusiness", "Bookkeeping"]},
    {"term": "bookkeeper trust issues",         "subreddits": ["smallbusiness", "Entrepreneur"]},
    {"term": "Pilot bookkeeping",               "subreddits": ["smallbusiness", "Bookkeeping"]},
    {"term": "bookkeeping app vs human",        "subreddits": ["smallbusiness", "Bookkeeping"]},
    {"term": "cheap bookkeeper mistake",        "subreddits": ["smallbusiness", "Bookkeeping"]},
    {"term": "bookkeeper didn't reconcile",     "subreddits": ["smallbusiness", "Bookkeeping", "QuickBooks"]},
]

TIERS = {
    "1_pain_language":  TIER_1_SEARCHES,
    "2_buying_intent":  TIER_2_SEARCHES,
    "3_icp_specific":   TIER_3_SEARCHES,
    "4_competitor":     TIER_4_SEARCHES,
}


# ---------------------------------------------------------------------------
# Source 1: old.reddit.com JSON (primary — more permissive than www)
# ---------------------------------------------------------------------------

def _fetch_old_reddit(subreddit: str, query: str, time_filter: str,
                      max_items: int, session: requests.Session) -> list[dict] | None:
    """Try old.reddit.com JSON endpoint. Returns None on hard failure (try fallback)."""
    url = (
        f"https://old.reddit.com/r/{subreddit}/search.json"
        f"?q={quote_plus(query)}&restrict_sr=1&t={time_filter}"
        f"&sort=relevance&limit={min(max_items, 100)}"
    )

    for attempt in range(MAX_RETRIES):
        try:
            # Add jitter to look less bot-like
            time.sleep(random.uniform(0.3, 1.0))
            resp = session.get(url, timeout=20)

            if resp.status_code == 429:
                wait = (attempt + 1) * 5 + random.uniform(1, 3)
                print(f"      Rate limited, waiting {wait:.0f}s...")
                time.sleep(wait)
                continue

            if resp.status_code == 403:
                # Reddit is blocking us — signal to try fallback
                return None

            if resp.status_code != 200:
                print(f"      old.reddit HTTP {resp.status_code} for r/{subreddit}")
                return None

            data = resp.json()
            children = data.get("data", {}).get("children", [])
            results = [child["data"] for child in children if child.get("data")]
            return results

        except (requests.RequestException, json.JSONDecodeError) as e:
            if attempt < MAX_RETRIES - 1:
                wait = (attempt + 1) * 3
                print(f"      old.reddit error: {e}, retrying in {wait}s...")
                time.sleep(wait)
            else:
                return None

    return None


# ---------------------------------------------------------------------------
# Source 2: PullPush.io API (fallback — Reddit archive, no auth)
# ---------------------------------------------------------------------------

def _fetch_pullpush(subreddit: str, query: str, time_filter: str,
                    max_items: int, session: requests.Session) -> list[dict]:
    """Fetch from PullPush.io Reddit archive API. Always returns a list."""
    after_ts = int(
        (datetime.now(tz=timezone.utc) - timedelta(days=TIME_FILTER_DAYS.get(time_filter, 30))).timestamp()
    )

    url = (
        f"https://api.pullpush.io/reddit/search/submission/"
        f"?q={quote_plus(query)}&subreddit={subreddit}"
        f"&after={after_ts}&sort=score&sort_type=desc"
        f"&size={min(max_items, 100)}"
    )

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=30)

            if resp.status_code == 429:
                wait = (attempt + 1) * 5
                print(f"      PullPush rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                print(f"      PullPush HTTP {resp.status_code} for r/{subreddit}")
                return []

            data = resp.json()
            return data.get("data", [])

        except (requests.RequestException, json.JSONDecodeError) as e:
            if attempt < MAX_RETRIES - 1:
                wait = (attempt + 1) * 3
                print(f"      PullPush error: {e}, retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"      PullPush failed after {MAX_RETRIES} attempts: {e}")
                return []

    return []


# ---------------------------------------------------------------------------
# Unified fetcher — tries old.reddit.com first, falls back to PullPush
# ---------------------------------------------------------------------------

# Track which source is working to avoid repeated failures
_source_status = {"old_reddit_failed": False}


def fetch_reddit_search(subreddit: str, query: str, time_filter: str,
                        max_items: int, session: requests.Session) -> list[dict]:
    """Fetch search results, trying old.reddit.com then PullPush.io."""

    # Try old.reddit.com first (unless it already failed this run)
    if not _source_status["old_reddit_failed"]:
        results = _fetch_old_reddit(subreddit, query, time_filter, max_items, session)
        if results is not None:
            if results:
                return results
            # Empty results from old.reddit is fine (just no matches for this query)
            return []
        else:
            print(f"      old.reddit blocked — switching to PullPush.io for remaining queries")
            _source_status["old_reddit_failed"] = True

    # Fallback to PullPush.io
    results = _fetch_pullpush(subreddit, query, time_filter, max_items, session)
    return results


def fetch_comments_for_post(post_id: str, subreddit: str,
                            session: requests.Session, max_comments: int = 10) -> list[dict]:
    """Fetch top comments for a post. Tries old.reddit.com, falls back to PullPush."""

    # Try old.reddit.com JSON
    if not _source_status["old_reddit_failed"]:
        url = f"https://old.reddit.com/r/{subreddit}/comments/{post_id}.json?limit={max_comments}&sort=top"
        try:
            time.sleep(random.uniform(0.3, 1.0))
            resp = session.get(url, timeout=20)
            if resp.status_code == 200:
                data = resp.json()
                if len(data) >= 2:
                    comments_data = data[1].get("data", {}).get("children", [])
                    comments = []
                    for child in comments_data:
                        if child.get("kind") != "t1":
                            continue
                        c = child.get("data", {})
                        if c.get("body") and len(c["body"]) > 30:
                            comments.append(c)
                    return comments[:max_comments]
        except (requests.RequestException, json.JSONDecodeError):
            pass

    # Fallback: PullPush comment search (searches by link_id)
    url = f"https://api.pullpush.io/reddit/search/comment/?link_id={post_id}&sort=score&sort_type=desc&size={max_comments}"
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return [c for c in data.get("data", []) if len(c.get("body", "")) > 30][:max_comments]
    except (requests.RequestException, json.JSONDecodeError):
        pass

    return []


def normalize_post(raw: dict, tier_name: str, search_term: str,
                   subreddit: str) -> dict:
    """Convert Reddit/PullPush JSON post to Apify-compatible format for the adapter."""
    # Handle both old.reddit.com format and PullPush format
    permalink = raw.get("permalink", "")
    if permalink and not permalink.startswith("http"):
        url = f"https://www.reddit.com{permalink}"
    elif permalink:
        url = permalink
    else:
        url = raw.get("full_link", raw.get("url", ""))

    created = raw.get("created_utc", 0)
    if isinstance(created, (int, float)) and created > 0:
        created_iso = datetime.fromtimestamp(created, tz=UTC).isoformat()
    else:
        created_iso = datetime.now(tz=UTC).isoformat()

    return {
        "id": raw.get("id", ""),
        "title": raw.get("title", ""),
        "body": raw.get("selftext", raw.get("body", "")),
        "url": url,
        "subreddit": raw.get("subreddit", subreddit),
        "createdAt": created_iso,
        "score": raw.get("score", 0),
        "numberOfComments": raw.get("num_comments", 0),
        "upvotes": raw.get("ups", raw.get("score", 0)),
        # Metadata tags (same as apify_scrape.py)
        "_tier": tier_name,
        "_search_term": search_term,
        "_target_subreddits": [subreddit],
    }


def normalize_comment(raw: dict, parent_post: dict, tier_name: str,
                      search_term: str, subreddit: str) -> dict:
    """Convert Reddit/PullPush JSON comment to Apify-compatible format."""
    permalink = raw.get("permalink", "")
    if permalink and not permalink.startswith("http"):
        url = f"https://www.reddit.com{permalink}"
    elif permalink:
        url = permalink
    else:
        url = ""

    created = raw.get("created_utc", 0)
    if isinstance(created, (int, float)) and created > 0:
        created_iso = datetime.fromtimestamp(created, tz=UTC).isoformat()
    else:
        created_iso = datetime.now(tz=UTC).isoformat()

    return {
        "id": raw.get("id", ""),
        "title": parent_post.get("title", ""),
        "body": raw.get("body", ""),
        "url": url,
        "subreddit": raw.get("subreddit", subreddit),
        "createdAt": created_iso,
        "score": raw.get("score", 0),
        "numberOfComments": 0,
        "upvotes": raw.get("ups", raw.get("score", 0)),
        "isComment": True,
        "parentId": parent_post.get("id", ""),
        "postBody": parent_post.get("selftext", parent_post.get("body", "")),
        "postTitle": parent_post.get("title", ""),
        "_tier": tier_name,
        "_search_term": search_term,
        "_target_subreddits": [subreddit],
    }


# ---------------------------------------------------------------------------
# Tier runner
# ---------------------------------------------------------------------------

def run_tier(session: requests.Session, tier_name: str, searches: list[dict],
             max_items: int, time_filter: str, include_comments: bool) -> list[dict]:
    """Run all searches for a single tier and collect results."""
    print(f"\n{'='*60}")
    print(f"  TIER: {tier_name}")
    print(f"  Searches: {len(searches)}")
    print(f"  Max items per search: {max_items}")
    print(f"  Time filter: {time_filter}")
    print(f"  Comments: {'yes' if include_comments else 'no'}")
    print(f"{'='*60}")

    all_results = []
    seen_ids = set()

    for i, search in enumerate(searches, 1):
        print(f"\n  [{i}/{len(searches)}] '{search['term']}' in {search['subreddits']}")

        for subreddit in search["subreddits"]:
            time.sleep(REQUEST_DELAY)

            posts = fetch_reddit_search(
                subreddit, search["term"], time_filter, max_items, session
            )

            new_posts = 0
            new_comments = 0

            for raw_post in posts:
                post_id = raw_post.get("id", "")
                if not post_id or post_id in seen_ids:
                    continue
                seen_ids.add(post_id)

                normalized = normalize_post(raw_post, tier_name, search["term"], subreddit)
                all_results.append(normalized)
                new_posts += 1

                # Fetch comments for high-engagement posts
                if include_comments and raw_post.get("num_comments", 0) >= 3:
                    time.sleep(REQUEST_DELAY)
                    comments = fetch_comments_for_post(post_id, subreddit, session)
                    for comment in comments:
                        cid = comment.get("id", "")
                        if cid and cid not in seen_ids:
                            seen_ids.add(cid)
                            norm_comment = normalize_comment(
                                comment, raw_post, tier_name, search["term"], subreddit
                            )
                            all_results.append(norm_comment)
                            new_comments += 1

            print(f"    r/{subreddit}: {new_posts} posts" +
                  (f", {new_comments} comments" if new_comments else ""))

    return all_results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="No-auth Reddit scraper for Zen Books VoC pipeline"
    )
    parser.add_argument("--tier", default="all",
                        help="Tier to run: all, 1_pain_language, 2_buying_intent, "
                             "3_icp_specific, 4_competitor")
    parser.add_argument("--max-items", type=int, default=25,
                        help="Max items per search query (default: 25, max: 100)")
    parser.add_argument("--time-filter", default="month",
                        help="Time filter: hour, day, week, month, year, all")
    parser.add_argument("--no-comments", action="store_true",
                        help="Skip comment fetching (faster)")
    args = parser.parse_args()

    # Determine tiers
    if args.tier == "all":
        tiers_to_run = TIERS
    elif args.tier in TIERS:
        tiers_to_run = {args.tier: TIERS[args.tier]}
    else:
        print(f"ERROR: Unknown tier '{args.tier}'. Options: all, {', '.join(TIERS.keys())}")
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)

    total_queries = sum(
        len(s["subreddits"]) for searches in tiers_to_run.values() for s in searches
    )
    est_minutes = (total_queries * REQUEST_DELAY) / 60

    print(f"No-Auth Reddit VoC Scraper — Zen Books")
    print(f"Sources: old.reddit.com JSON -> PullPush.io fallback")
    print(f"Tiers: {', '.join(tiers_to_run.keys())}")
    print(f"Max items/query: {args.max_items}, Time: {args.time_filter}")
    print(f"Total API calls: ~{total_queries} ({est_minutes:.1f} min est.)")
    print(f"Comments: {'off' if args.no_comments else 'on (adds ~3s per high-engagement post)'}")

    grand_total = []

    for tier_name, searches in tiers_to_run.items():
        results = run_tier(
            session, tier_name, searches,
            args.max_items, args.time_filter,
            include_comments=not args.no_comments,
        )

        # Save per-tier results (same format as apify_scrape.py)
        tier_file = OUTPUT_DIR / f"{tier_name}.json"
        with open(tier_file, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\n  Saved {len(results)} items to {tier_file}")

        grand_total.extend(results)

    # Save combined results
    combined_file = OUTPUT_DIR / "all_tiers_combined.json"
    with open(combined_file, "w") as f:
        json.dump(grand_total, f, indent=2, default=str)

    print(f"\n{'='*60}")
    print(f"  COMPLETE: {len(grand_total)} total items across {len(tiers_to_run)} tier(s)")
    print(f"  Combined output: {combined_file}")
    print(f"  Next: python scripts/apify_adapter.py  (format conversion)")
    print(f"        python scripts/apify_import.py   (load into DB)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
