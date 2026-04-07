FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

# Optional extra deps for solver_core (if present)
RUN if [ -f solver_core/requirements.txt ]; then pip install --no-cache-dir -r solver_core/requirements.txt; fi

ENV PYTHONPATH=/app

EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
