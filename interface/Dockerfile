FROM python:3.12-slim

WORKDIR /app

COPY interface/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY interface/ /app/interface/

EXPOSE 8501
CMD ["streamlit", "run", "/app/interface/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
