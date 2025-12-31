# Fake News Detection

## Описание

Сервис предназначен для автоматического обнаружения фейковых новостей на основе текстового контента.  
Решение построено как end-to-end MLOps pipeline. Пользователь или внешний источник отправляет новости (одиночные или CSV-файлы) в Kafka. Далее сервис скоринга применяет предобученную модель машинного обучения и сохраняет результаты в PostgreSQL. Результаты доступны через UI и визуализируются в Grafana.
---

## Архитектура проекта
Данные поступают в Kafka в топик raw_news.
Сервис Fake News Scorer читает сообщения из Kafka и выполняет ML-инференс на CPU.
Результаты скоринга публикуются обратно в Kafka в топик scored_news.
Отдельный сервис Database Logger читает результаты из Kafka и сохраняет их в PostgreSQL в таблицу news_predictions.
Streamlit UI и Grafana используют данные из PostgreSQL для отображения истории предсказаний и агрегированных метрик.

Dagster используется для периодических оффлайн-расчётов агрегированных статистик на основе данных в PostgreSQL.

## Структура репозитория
```
docker-compose.yml  

news_database/  
  init.sql  

news_scorer/  
  Dockerfile  
  app.py  
  loader.py  
  preprocessing.py  
  requirements.txt  

database_logger/  
  app.py  
  Dockerfile  
  requirements.txt  

interface/  
  Dockerfile  
  app.py  
  requirements.txt  

dagster/  
  dagster_calc/ 
    assets.py  
    jobs.py  
    schedules.py  
    repository.py  
  Dockerfile 
  requirements.txt 
  workspace.yaml 

model/  
  model.pkl  
  vectorizer.pkl  

```

## ML-модель

Используется классическая задача классификации текста:

- TF-IDF векторизация
- Линейная модель
- Inference выполняется только на CPU

Препроцессинг включает:
- приведение текста к нижнему регистру
- удаление спецсимволов
- лемматизацию
- удаление stopwords

---

## Компоненты системы

Kafka  
- raw_news — входные данные  
- scored_news — результаты скоринга  

PostgreSQL  
- таблица news_predictions (score, is_fake, created_at)

Streamlit UI  
- отправка одиночных новостей
- загрузка CSV
- просмотр истории предсказаний
- гистограмма распределения скоров

Dagster  
- периодический пересчёт агрегатов
- scheduler / ETL слой

Grafana  
- Fake vs Real (pie chart)
- Score distribution (histogram)

---

## Запуск проекта

Требования:
- Docker 20.10+
- Docker Compose

Запуск:
```
docker compose up --build
```
После запуска будут доступны:
- Streamlit UI: http://localhost:8501
- Kafka UI: http://localhost:8080
- Dagster UI: http://localhost:3000
- Grafana: http://localhost:3001 (admin / admin)

---

## Бизнес-ценность

Сервис может использоваться новостными агрегаторами для автоматического выявления фейкового контента, снижения нагрузку на сектор модерации.

