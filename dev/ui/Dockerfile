# Dev-only web console — stdlib-only static server + /api reverse proxy (no dependencies).
FROM python:3.12-slim
WORKDIR /app
COPY server.py ./
COPY static ./static
EXPOSE 8090
CMD ["python", "server.py"]
