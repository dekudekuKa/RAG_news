from pymongo import MongoClient
from text_cleaner import clean_text

client = MongoClient("mongodb://localhost:27017")
db = client["news_database"]
collection = db["articles"]

update_count = 0

for doc in collection.find():
    if "content" in doc:
        cleaned_content = clean_text(doc["content"])
        collection.update_one({"_id": doc["_id"]}, {"$set": {"content": cleaned_content}})
        update_count += 1

print(f"Updated {update_count} documents with cleaned content.")