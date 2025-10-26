import os
import asyncio
import aiohttp
import asyncpg
import time
from dotenv import load_dotenv

load_dotenv()

GITHUB_API = "https://api.github.com/graphql"
TOKEN = os.getenv("GITHUB_TOKEN")

headers = {"Authorization": f"Bearer {TOKEN}"}

DB_CONFIG = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "database": os.getenv("POSTGRES_DB"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
}

# --- define multiple star ranges to bypass 1000-result cap ---
STAR_RANGES = [
    "stars:10..50",
    "stars:51..100",
    "stars:101..200",
    "stars:201..500",
    "stars:501..1000",
    "stars:1001..5000",
    "stars:5001..10000",
    "stars:10001..50000",
    "stars:50001..100000",
    "stars:>100000"
]


# ---------------------------
# Fetch one page of repos
# ---------------------------
async def fetch_repos(session, cursor=None, star_filter="stars:>10"):
    query = """
    query ($cursor: String, $filter: String!) {
      search(query: $filter, type: REPOSITORY, first: 100, after: $cursor) {
        edges {
          node {
            ... on Repository {
              id
              name
              owner { login }
              stargazerCount
            }
          }
        }
        pageInfo {
          endCursor
          hasNextPage
        }
      }
    }
    """
    variables = {"cursor": cursor, "filter": star_filter}
    async with session.post(GITHUB_API, json={"query": query, "variables": variables}) as response:
        if response.status != 200:
            print(f"⚠️ GitHub API error {response.status} for {star_filter}")
            text = await response.text()
            print(text)
            return None
        data = await response.json()
        return data["data"]["search"]


# ---------------------------
# Save to PostgreSQL
# ---------------------------
async def save_to_db(pool, repos):
    async with pool.acquire() as conn:
        async with conn.transaction():
            for repo in repos:
                node = repo["node"]
                await conn.execute("""
                INSERT INTO repositories (repo_id, name, owner, stars_count)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (repo_id) DO UPDATE
                SET stars_count = EXCLUDED.stars_count,
                    last_updated = NOW();
            """, node["id"], node["name"], node["owner"]["login"], node["stargazerCount"])


# ---------------------------
# Main crawl logic
# ---------------------------
async def crawl():
    start_time = time.time()
    total_repos = 0

    pool = await asyncpg.create_pool(**DB_CONFIG)
    async with aiohttp.ClientSession(headers=headers) as session:
        for star_filter in STAR_RANGES:
            print(f"\n🚀 Crawling range: {star_filter}")
            cursor = None
            page_count = 0
            while True:
                result = await fetch_repos(session, cursor, star_filter)
                if not result:
                    print("⚠️ No data returned, waiting 15s...")
                    await asyncio.sleep(15)
                    continue

                await save_to_db(pool, result["edges"])
                total_repos += len(result["edges"])
                page_count += 1
                print(f"✅ {star_filter} | Page {page_count} | Total repos: {total_repos}")

                cursor = result["pageInfo"]["endCursor"]
                if not result["pageInfo"]["hasNextPage"]:
                    break

                await asyncio.sleep(1.5)  # respect rate limits

    await pool.close()
    elapsed = time.time() - start_time
    print(f"\n🎉 Finished crawling {total_repos} repositories in {elapsed/60:.2f} minutes.")


if __name__ == "__main__":
    asyncio.run(crawl())
