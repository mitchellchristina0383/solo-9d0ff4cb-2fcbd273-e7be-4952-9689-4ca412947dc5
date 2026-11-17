FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    RECEIVER_HOST=0.0.0.0 \
    RECEIVER_PORT=9000

WORKDIR /app
COPY server.py ./server.py

EXPOSE 9000

HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9000/health', timeout=2)"

CMD ["python", "server.py"]
