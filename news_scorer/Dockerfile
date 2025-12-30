FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*


RUN mkdir -p /app/logs && \
    touch /app/logs/service.log && \
    chmod -R 777 /app/logs

COPY news_scorer/requirements.txt ./news_scorer/requirements.txt
RUN pip install --no-cache-dir -r ./news_scorer/requirements.txt

RUN python -c "import nltk; nltk.download('stopwords'); nltk.download('wordnet')"

COPY news_scorer/ ./news_scorer/
COPY model/ ./model/

ENV WEIGHTS_DIR=/app/model

CMD ["python", "-m", "news_scorer.app"]
