FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir pandas pika

COPY producer.py /app/producer.py

CMD ["python", "producer.py"]
