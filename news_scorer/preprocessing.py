import re
from typing import Optional

import pandas as pd
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

STOP_WORDS = set(stopwords.words("english"))
LEMMATIZER = WordNetLemmatizer()


def parse_el(text):
    if text is None:
        return ""
    else:
        return str(text)


def combine_text(title, text):
    return (parse_el(title) + " " + parse_el(text)).strip()




def clean_text(text):
    text = str(text).lower()
    text = re.sub(r"[^a-z\s]", "", text)

    words = [LEMMATIZER.lemmatize(word) for word in text.split() if word not in STOP_WORDS]
    return " ".join(words)


def preprocess_message(payload):
    if "text" not in payload:
        raise ValueError("Missing 'text' in payload")

    return clean_text(combine_text(payload.get("title"), payload.get("text")))


#def preprocess_dataframe(df: pd.DataFrame) -> pd.Series:
#    if "title" in df.columns:
#        combined = df.apply(
#            lambda r: combine_text(r.get("title"), r.get("text")),
#            axis=1,
#        )
#    else:
#        combined = df["text"].astype(str)
#
#    return combined.apply(clean_text)
