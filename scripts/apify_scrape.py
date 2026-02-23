"""
apify_scrape.py — Apify Reddit scraper for Zen Books VoC pipeline

Runs the trudax/reddit-scraper actor with 4-tier search queries targeting
bookkeeping pain points across our 4 ICPs (real estate, therapist, nonprofit,
restaurant/construction).

Called by the GitHub Action workflow, or run locally:
    APIFY_API_TOKEN=your_token TIER=all MAX_ITEMS=100 TIME_FILTER=month python scripts/apify_scrape.py
"""

import json
import os
import sys
import time
from pathlib import Path
from apify_client import ApifyClient

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

APIFY_TOKEN = os.environ.get("APIFY_API_TOKEN", "")
TIER = os.environ.get("TIER", "all")
MAX_ITEMS = int(os.environ.get("MAX_ITEMS", "100"))
TIME_FILTER = os.environ.get("TIME_FILTER", "month")
ACTOR_ID = "trudax/reddit-scraper"
OUTPUT_DIR = Path("data/apify_results")

# ---------------------------------------------------------------------------
# 4-Tier search queries — Zen Books VoC strategy
# ---------------------------------------------------------------------------

# Tier 1: Pain language mining
# Raw bookkeeping/accounting struggle posts — emotional, quotable language
TIER_1_SEARCHES = [
    # Universal bookkeeping pain
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

# Tier 2: Solution-seeking / buying intent
# People actively looking for bookkeeping help — high purchase intent
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

# Tier 3: ICP-specific pain
# Targeted queries per niche — maps directly to zen.books spoke pages
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

# Tier 4: Competitor complaints
# Dissatisfaction with alternatives — objection handling and differentiation copy
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
# Apify runner
# ---------------------------------------------------------------------------

def build_start_urls(search: dict) -> list[dict]:
    """Build Reddit search URLs for the actor's startUrls input."""
    urls = []
    for subreddit in search["subreddits"]:
        url = f"https://www.reddit.com/r/{subreddit}/search/?q={search['term']}&restrict_sr=1&t={TIME_FILTER}&sort=relevance"
        urls.append({"url": url})
    return urls


def run_tier(client: ApifyClient, tier_name: str, searches: list[dict]) -> list[dict]:
    """Run all searches for a single tier and collect results."""
    print(f"\n{'='*60}")
    print(f"  TIER: {tier_name}")
    print(f"  Searches: {len(searches)}")
    print(f"  Max items per search: {MAX_ITEMS}")
    print(f"  Time filter: {TIME_FILTER}")
    print(f"{'='*60}")

    all_results = []

    for i, search in enumerate(searches, 1):
        print(f"\n  [{i}/{len(searches)}] '{search['term']}' in {search['subreddits']}")

        start_urls = build_start_urls(search)

        run_input = {
            "startUrls": start_urls,
            "maxItems": MAX_ITEMS,
            "maxPostCount": MAX_ITEMS,
            "maxComments": 50,
            "sort": "relevance",
            "time": TIME_FILTER,
        }

        try:
            run = client.actor(ACTOR_ID).call(run_input=run_input, timeout_secs=300)
            dataset_id = run["defaultDatasetId"]

            items = list(client.dataset(dataset_id).iterate_items())
            print(f"    -> {len(items)} items collected")

            # Tag each item with tier and search metadata
            for item in items:
                item["_tier"] = tier_name
                item["_search_term"] = search["term"]
                item["_target_subreddits"] = search["subreddits"]

            all_results.extend(items)

        except Exception as e:
            print(f"    -> ERROR: {e}")
            continue

    return all_results


def main():
    if not APIFY_TOKEN:
        print("ERROR: APIFY_API_TOKEN environment variable not set")
        sys.exit(1)

    client = ApifyClient(APIFY_TOKEN)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Determine which tiers to run
    if TIER == "all":
        tiers_to_run = TIERS
    elif TIER in TIERS:
        tiers_to_run = {TIER: TIERS[TIER]}
    else:
        print(f"ERROR: Unknown tier '{TIER}'. Options: all, {', '.join(TIERS.keys())}")
        sys.exit(1)

    print(f"Apify Reddit VoC Scraper — Zen Books")
    print(f"Tiers: {', '.join(tiers_to_run.keys())}")
    print(f"Actor: {ACTOR_ID}")
    print(f"Max items/query: {MAX_ITEMS}, Time: {TIME_FILTER}")

    grand_total = []

    for tier_name, searches in tiers_to_run.items():
        results = run_tier(client, tier_name, searches)

        # Save per-tier results
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
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
