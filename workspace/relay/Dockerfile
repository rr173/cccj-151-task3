FROM python:3.11-slim

WORKDIR /srv/relay
COPY app ./app
COPY web ./web

ENV RELAY_PORT=8090 \
    RELAY_DB=/srv/relay/data/relay.db \
    PYTHONUNBUFFERED=1

EXPOSE 8090
VOLUME ["/srv/relay/data"]

HEALTHCHECK --interval=10s --timeout=3s --retries=10 \
  CMD python3 -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8090/api/health', timeout=2).status==200 else 1)"

CMD ["python3", "app/server.py"]
