import re
import nltk


nltk.download('stopwords')
from nltk.corpus import stopwords

stop_words = set(stopwords.words('ukrainian') + stopwords.words('russian'))

morph = nltk.MorphAnalyzer()

def clean_text(text):
    text = re.sub(r"[^А-Яа-яЇїІіЄєҐґA-Za-z\s]", " ", text)

    text = text.lower()

    tokens = text.split()

    tokens = [word for word in tokens if word not in stop_words and len(word) > 2]

    lemmatized = [morph.parse(token)[0].normal_form for token in tokens]

    return " ".join(lemmatized)

