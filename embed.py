from pymongo import MongoClient
from bson.objectid import ObjectId
from sentence_transformers import SentenceTransformer
import datetime
import numpy as np

client = MongoClient("mongodb://localhost:27017")
db = client["news_database"]
clean_collection = db["clean_articles"]
vectors_collection = db["vectorized_articles"]
model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")


def vectorize_text(text):
    embedding = model.encode(text)
    return embedding.tolist()


def process_vectors():
    total = 0
    for doc in clean_collection.find():
        print(f"👀 Перевіряємо документ: {doc.get('raw_id', doc['_id'])}")

        if vectors_collection.find_one({"clean_id": doc["_id"]}):
            print(f"⏩ Вже векторизований: {doc['_id']}")
            continue

        clean_text = doc.get("clean_text", "")
        if not clean_text.strip():
            print(f"⚠️ Порожній clean_text у документі: {doc['_id']}")
            continue

        vector = vectorize_text(clean_text)

        vector_doc = {
            "clean_id": doc["_id"],
            "vector": vector,
            "vectorized_at": datetime.datetime.utcnow()
        }

        vectors_collection.insert_one(vector_doc)
        print(f"✅ Векторизовано: {doc.get('raw_id', doc['_id'])}")
        total += 1

    print(f"\n🏁 Завершено. Всього векторизовано: {total}")


if __name__ == "__main__":
    process_vectors()

