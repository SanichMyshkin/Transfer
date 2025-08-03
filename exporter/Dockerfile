FROM python:3.11-slim

RUN python3 -m venv /app/.venv

WORKDIR /app
COPY . .

RUN /app/.venv/bin/pip install --no-cache-dir -r requirements.txt


ENV VIRTUAL_ENV=/app/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

EXPOSE 8000

CMD ["python", "main.py"]


# FROM python:3.11-slim


# RUN apt-get update && apt-get install -y --no-install-recommends \
#     ca-certificates \
#     && rm -rf /var/lib/apt/lists/*


# COPY CERT_NAME /usr/local/share/ca-certificates/nexus.crt
# RUN update-ca-certificates

# RUN python3 -m venv /app/.venv

# WORKDIR /app
# COPY . .
# RUN /app/.venv/bin/pip install --no-cache-dir -r requirements.txt

# ENV VIRTUAL_ENV=/app/.venv
# ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# EXPOSE 8000
# CMD ["python", "main.py"]
