# Separate from the API image on purpose. The consumer needs the scoring stack
# — numpy, pandas, scikit-learn, xgboost — which is a few hundred MB that the
# API has no use for, and the API image is what `migrate` and `create-buckets`
# also reuse. Keeping them apart means a model dependency bump cannot force a
# redeploy of the API or the migration runner.
FROM python:3.14-slim

WORKDIR /code

# Same reason as the API image: bcrypt/psycopg2 and some of the scientific
# wheels may not ship cp314 builds, and gcc covers the build-from-source path.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

# Two requirements files, one pip invocation, so the resolver sees both sets at
# once and cannot pick a version for a shared transitive dependency that
# satisfies one file and breaks the other.
COPY requirements.txt ./requirements.txt
COPY ml/requirements.txt ./ml-requirements.txt
RUN pip install --no-cache-dir -r requirements.txt -r ml-requirements.txt

# consumers.py reaches into all three: ml.train_fraud_model for featurize(),
# app.database.connection for the engine, and app.routers.transactions for the
# shared Redis client and idempotency manager.
COPY app ./app
COPY ml ./ml
COPY kafka ./kafka

# PYTHONPATH is load-bearing rather than cosmetic. `python kafka/consumers.py`
# puts /code/kafka on sys.path — not /code — so `from ml.train_fraud_model
# import featurize` raises ModuleNotFoundError. Setting it to the project root
# is what makes the script form work; the alternative is `python -m`, which
# needs a kafka/__init__.py that does not exist.
#
# Note the model is loaded as the relative path ml/models/fraud_model.json, so
# the process has to start with /code as its working directory. WORKDIR above
# is what guarantees that.
ENV PYTHONPATH=/code \
    PYTHONUNBUFFERED=1

RUN useradd --create-home appuser
USER appuser

CMD ["python", "kafka/consumers.py"]
