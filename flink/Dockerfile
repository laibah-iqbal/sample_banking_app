# One image serves all three Flink roles — jobmanager, taskmanager and the
# job submitter — because all three need the same two things on disk: the
# connector jar and the Python job code.
#
# The tag is pinned to the exact version PyFlink is built against. A client
# built against a different Flink minor than the cluster fails at submission
# with deserialization errors that never mention the version as the cause, so
# flink:2.3.0 and apache-flink==2.3.0 move together or not at all.
FROM flink:2.3.0-java17

# PyFlink publishes no linux/aarch64 wheel — PyPI has manylinux x86_64 and
# macOS arm64 only. On Apple Silicon pip would therefore fall back to the
# sdist and try to compile Apache Beam's C extensions, which is why the Flink
# services in docker-compose.yml pin `platform: linux/amd64` and run under
# emulation. Slower to start, but it uses the published wheel.
# The base image's default user is `flink`, not root, so apt and pip both fail
# with "Permission denied" without this. Dropped back to flink before the end —
# the entrypoint expects to run as that user.
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# --break-system-packages because the base is Debian bookworm, whose system
# Python is marked externally managed (PEP 668). Nothing else in this image
# uses that interpreter, so a venv would add indirection and a PATH problem
# for `flink run` without buying isolation from anything.
RUN pip3 install --no-cache-dir --break-system-packages \
        "apache-flink==2.3.0" \
        "sqlmodel==0.0.47"

# The connector goes on the cluster classpath instead of being shipped per-job
# through pipeline.jars. It is needed in two places: by the Table planner in
# the submitter, which resolves 'connector' = 'kafka' to a factory while
# building the job graph, and by the TaskManagers at runtime. Since this one
# image is all of those, a single copy into /opt/flink/lib covers them.
COPY flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar /opt/flink/lib/

# create_enriched_stream.py derives its DDL from the SQLModel table
# definitions, so it imports app.database.schemas. That module imports only
# sqlmodel, sqlalchemy and the standard library — no config, no database
# connection at import time — which is why this image installs sqlmodel alone
# rather than the API's whole requirements.txt.
WORKDIR /opt/banking
COPY flink ./flink
COPY app ./app

# Makes `flink.create_enriched_stream` importable as a module. It has to run as
# a module, not a script: the job does `from . import utils`, and a relative
# import has no parent package when the file is executed by path.
ENV PYTHONPATH=/opt/banking

USER flink
