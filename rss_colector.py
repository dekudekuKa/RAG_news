import feedparser
from newspaper import Article
from pymongo import MongoClient
from datetime import datetime
import time

# ==== 1. Налаштування ====

RSS_FEEDS = [
    "https://www.pravda.com.ua/rss/",
    "https://www.epravda.com.ua/rss/"
]

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "news_database"
COLLECTION_NAME = "raw_articles"
SLEEP_BETWEEN_REQUESTS = 1  # сек

# ==== 2. Підключення до MongoDB ====

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# ==== 3. Завантаження повного тексту статті ====

def fetch_article_content(url):
    try:
        article = Article(url)
        article.download()
        article.parse()
        return article.text
    except Exception as e:
        print(f"[⚠️ Error fetching article] {url}: {e}")
        return ""

# ==== 4. Основна функція збору новин ====

def collect_and_store_news():
    total_added = 0
    for feed_url in RSS_FEEDS:
        print(f"\n🔗 Loading feed: {feed_url}")
        feed = feedparser.parse(feed_url)

        for entry in feed.entries:
            article = {
                "title": entry.get("title", "").strip(),
                "link": entry.get("link", "").strip(),
                "published": entry.get("published", ""),
                "summary": entry.get("summary", "").strip(),
                "source": feed.feed.get("title", ""),
                "collected_at": datetime.utcnow()
            }

            if collection.find_one({"link": article["link"]}):
                print(f"⏩ Already exists: {article['title']}")
                continue

            article["content"] = fetch_article_content(article["link"])

            if article["content"]:
                collection.insert_one(article)
                print(f"✅ Added: {article['title']}")
                total_added += 1
            else:
                print(f"⚠️ Skipped (no content): {article['title']}")

            time.sleep(SLEEP_BETWEEN_REQUESTS)

    print(f"\n✅ Collection complete. Total added: {total_added}")

# ==== 5. Запуск ====

if __name__ == "__main__":
    collect_and_store_news()
