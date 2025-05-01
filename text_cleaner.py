import re
from pymongo import MongoClient
from bson.objectid import ObjectId
import stanza
import datetime

# 🔹 Підключення до MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["news_database"]
raw_collection = db["raw_articles"]
clean_collection = db["clean_articles"]

# 🔹 Завантаження української NLP моделі
stanza.download('uk')  # тільки при першому запуску
nlp = stanza.Pipeline('uk', processors='tokenize,mwt,pos,lemma', use_gpu=False)

# 🔹 Власні стоп-слова
ukrainian_stopwords = {
    'і', 'в', 'у', 'та', 'на', 'з', 'що', 'це', 'до', 'по', 'я', 'ми', 'ви', 'вони',
    'про', 'як', 'за', 'від', 'для', 'не', 'а', 'але', 'ще', 'бути', 'такий', 'ж',
    'так', 'його', 'її', 'їх', 'чи', 'який', 'яка', 'яке', 'які', 'тут', 'там',
    'через', 'після', 'перед', 'між', 'теж', 'ж', 'ото', 'ні', 'також', 'хто', 'коли',
    'чому', 'де', 'із', 'над', 'під', 'об', 'тощо', 'щоб', 'саме'
}

# 🔹 Функція очищення й лематизації
def clean_text(text):
    text = re.sub(r"[^А-Яа-яЇїІіЄєҐґ\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    doc = nlp(text)
    lemmas = []

    for sentence in doc.sentences:
        for word in sentence.words:
            lemma = word.lemma.lower()
            if lemma not in ukrainian_stopwords and len(lemma) > 2:
                lemmas.append(lemma)

    return " ".join(lemmas), lemmas  # і строка, і список токенів

# 🔹 Основна обробка
def process_articles():
    raw_docs = raw_collection.find()
    count = 0
    skipped = 0

    for raw in raw_docs:
        # Пріоритет: text > content > summary
        text = raw.get("text") or raw.get("content") or raw.get("summary", "")
        if not text.strip():
            print(f"⚠ Пропущено порожній текст: {raw.get('_id')}")
            skipped += 1
            continue

        if clean_collection.find_one({"raw_id": raw["_id"]}):
            skipped += 1
            continue

        cleaned_str, tokens = clean_text(text)
        if not cleaned_str.strip():
            print(f"⚠ Порожній результат після обробки: {raw.get('_id')}")
            skipped += 1
            continue

        clean_doc = {
            "raw_id": raw["_id"],
            "clean_text": cleaned_str,
            "tokens": tokens,
            "lemmatized_at": datetime.datetime.utcnow()
        }

        clean_collection.insert_one(clean_doc)
        print(f"✓ Оброблено: {raw.get('title', '')}")
        count += 1

    print(f"\n✅ Завершено. Успішно: {count}, Пропущено: {skipped}")



if __name__ == "__main__":
    process_articles()
