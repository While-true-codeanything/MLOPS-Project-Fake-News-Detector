from pathlib import Path
import joblib


def load_model(path):
    return joblib.load(Path(path) / "model.pkl")


def load_vectorizer(path):
    return joblib.load(Path(path) / "vectorizer.pkl")


def load_artifacts(path: str):
    model = load_model(path)
    vectorizer = load_vectorizer(path)
    return model, vectorizer
