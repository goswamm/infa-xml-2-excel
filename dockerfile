# Render-ready FastAPI container
FROM python:3.11-slim

# Fonts for matplotlib (PDF rendering)
RUN apt-get update && apt-get install -y --no-install-recommends fonts-dejavu-core && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./ 
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Render sets $PORT; bind uvicorn to it
ENV PORT=10000
EXPOSE 10000

CMD exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT}
