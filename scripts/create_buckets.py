"""Create the MinIO buckets the pipeline writes into.

Replaces the old `mc mb` approach. MinIO now distributes its community edition
as source only and has withdrawn public access to its official container
images, so depending on a third-party `mc` image is fragile — and the
`alpine/minio` server image we do use ships no `mc` binary at all. Talking to
the S3 API directly needs no extra image and nothing to re-pull.

Idempotent: existing buckets are left untouched, so this is safe to re-run on
every `docker compose up`.
"""

import os
import sys

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
BUCKETS = [
    name.strip()
    for name in os.environ.get("MINIO_BUCKETS", "bronze,warehouse").split(",")
    if name.strip()
]


def main() -> int:
    try:
        access_key = os.environ["MINIO_ROOT_USER"]
        secret_key = os.environ["MINIO_ROOT_PASSWORD"]
    except KeyError as exc:
        print(f"missing required env var: {exc.args[0]}", file=sys.stderr)
        return 1

    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        # MinIO serves minio:9000/<bucket>, not <bucket>.minio:9000, so the
        # default virtual-host addressing fails to resolve. Same reason the
        # Kafka S3 sink needs s3.path.style.access.enabled.
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name=os.environ.get("MINIO_REGION", "us-east-1"),
    )

    # Doubles as a credential check: wrong keys fail here rather than leaving a
    # half-created set of buckets behind.
    try:
        existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    except (ClientError, BotoCoreError) as exc:
        print(f"cannot reach MinIO at {ENDPOINT}: {exc}", file=sys.stderr)
        return 1

    for bucket in BUCKETS:
        if bucket in existing:
            print(f"{bucket}: already exists")
            continue
        try:
            s3.create_bucket(Bucket=bucket)
            print(f"{bucket}: created")
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            # Lost a race with a concurrent run; the bucket is there either way.
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                print(f"{bucket}: already exists")
                continue
            print(f"{bucket}: FAILED ({code}) {exc}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
