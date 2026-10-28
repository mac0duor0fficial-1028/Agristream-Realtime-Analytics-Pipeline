FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sensor_sim.py producer.py ./

RUN useradd --system --uid 10001 producer
USER producer

CMD ["python", "producer.py"]
