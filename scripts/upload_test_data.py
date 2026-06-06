#!/usr/bin/env python3
"""
Upload sample data to the raw S3 bucket to test the pipeline end-to-end.
Run AFTER: terraform apply

Usage:
    python scripts/upload_test_data.py
    python scripts/upload_test_data.py --bucket my-custom-bucket
"""
import argparse
import json
import os
import subprocess
import sys
import boto3
from pathlib import Path

ROOT = Path(__file__).parent.parent


def get_raw_bucket() -> str:
    """Pull the raw bucket name from Terraform outputs."""
    result = subprocess.run(
        ["terraform", "output", "-json", "raw_bucket_name"],
        cwd=ROOT / "terraform",
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print("ERROR: terraform output failed. Did you run 'terraform apply'?")
        print(result.stderr)
        sys.exit(1)
    return json.loads(result.stdout)


def upload_samples(bucket: str) -> None:
    s3 = boto3.client("s3")
    samples = [
        (ROOT / "sample_data" / "orders.csv",  "uploads/orders.csv"),
        (ROOT / "sample_data" / "events.json", "uploads/events.json"),
    ]

    for local_path, s3_key in samples:
        if not local_path.exists():
            print(f"  SKIP  {local_path} (not found)")
            continue
        s3.upload_file(str(local_path), bucket, s3_key)
        print(f"  OK    s3://{bucket}/{s3_key}")

    print("\nDone. Check Lambda logs and Glue job runs in the AWS Console.")


def main():
    parser = argparse.ArgumentParser(description="Upload sample data to trigger the ETL pipeline")
    parser.add_argument("--bucket", help="Override raw bucket name (skips terraform output)")
    args = parser.parse_args()

    bucket = args.bucket or get_raw_bucket()
    print(f"Uploading to: s3://{bucket}/uploads/\n")
    upload_samples(bucket)


if __name__ == "__main__":
    main()
