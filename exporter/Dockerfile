FROM python:3.11-slim

RUN apt-get update && apt-get install -y ca-certificates

COPY ./certs /usr/local/share/ca-certificates/
RUN update-ca-certificates

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .

CMD ["python", "main.py"]



# RUN apt-get update && apt-get install -y \
#     ca-certificates \
#     && update-ca-certificates \
#     && apt-get install -y \
#     gcc \
#     libffi-dev \
#     libpq-dev \
#     && rm -rf /var/lib/apt/lists/*
 

# RUN python3 -m venv /app/.venv

# WORKDIR /app
# COPY . .

# RUN /app/.venv/bin/pip install --no-cache-dir -r requirements.txt

# ENV VIRTUAL_ENV=/app/.venv
# ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# EXPOSE 8000

# CMD ["python", "main.py"]