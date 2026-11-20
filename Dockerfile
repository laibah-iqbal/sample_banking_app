FROM python:3.14-slim

WORKDIR /code

# bcrypt/psycopg2/uvloop/httptools may not yet ship cp314 wheels; gcc covers
# the fallback build-from-source case.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
# Baked into the image rather than bind-mounted so the image can migrate any
# database it is pointed at, not just this compose project. alembic.ini sets
# prepend_sys_path = %(here)s, which resolves to /code here, so alembic/env.py
# can `import app.database...` with WORKDIR as it is.
# Adding a revision therefore means rebuilding: docker compose build migrate
COPY alembic.ini .
COPY alembic ./alembic

RUN useradd --create-home appuser
USER appuser

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
