#!/usr/bin/env bash
# package_upload.sh
# Usage:
#   ./package_upload.sh --bucket my-bucket --prefix etl-jobs/top-produce-etl --package top-produce-etl.zip \
#       [--profile my-aws-profile] [--region us-east-1] [--requirements prod-requirements.txt] [--version v1.0.0]

set -euo pipefail

# ---- Defaults ----
AWS_PROFILE=""
AWS_REGION="us-east-1"
S3_BUCKET="myjustinbucket51562"
S3_PREFIX="etl-jobs/top-produce-etl"
PACKAGE_NAME="top-produce-etl.zip"
REQUIREMENTS_FILE="prod-requirements.txt"
VERSION="1.0"
KEEP_TEMP=false

# ---- helper ----
usage() {
  cat <<EOF
Usage: $0 --bucket <bucket> --prefix <prefix> [options]
Options:
  --bucket       S3 bucket name (required)
  --prefix       S3 prefix/folder (required)
  --package      package name (default: ${PACKAGE_NAME})
  --requirements requirements file path (default: ${REQUIREMENTS_FILE})
  --profile      aws cli profile name (optional)
  --region       aws region (optional)
  --version      version tag to append to package name (recommended)
  --keep-temp    do not remove temp dir (debug)
EOF
  exit 1
}

# ---- parse args ----
while [[ $# -gt 0 ]]; do
  case "$1" in
    --bucket) S3_BUCKET="$2"; shift 2;;
    --prefix) S3_PREFIX="$2"; shift 2;;
    --package) PACKAGE_NAME="$2"; shift 2;;
    --requirements) REQUIREMENTS_FILE="$2"; shift 2;;
    --profile) AWS_PROFILE="$2"; shift 2;;
    --region) AWS_REGION="$2"; shift 2;;
    --version) VERSION="$2"; shift 2;;
    --keep-temp) KEEP_TEMP=true; shift 1;;
    *) echo "Unknown arg: $1"; usage;;
  esac
done

if [[ -z "$S3_BUCKET" || -z "$S3_PREFIX" ]]; then
  echo "bucket and prefix are required"
  usage
fi

if [[ -n "$VERSION" ]]; then
  PACKAGE_NAME="${PACKAGE_NAME%.zip}-$VERSION.zip"
fi

AWS_CMD="aws"
if [[ -n "$AWS_PROFILE" ]]; then
  AWS_CLI_ARGS+=(--profile "$AWS_PROFILE")
fi
if [[ -n "$AWS_REGION" ]]; then
  AWS_CLI_ARGS+=(--region "$AWS_REGION")
fi

# check aws cli
if ! command -v $AWS_CMD >/dev/null 2>&1; then
  echo "aws cli not found. Install and configure aws cli before running."
  exit 2
fi

TEMP_DIR=$(mktemp -d)
echo "Using temp dir: $TEMP_DIR"

cleanup() {
  if [[ "$KEEP_TEMP" != true ]]; then
    rm -rf "$TEMP_DIR"
    echo "Cleaned temp dir"
  else
    echo "Keeping temp dir for debug: $TEMP_DIR"
  fi
}
trap cleanup EXIT

# copy project files (adjust these to your project layout)
echo "Copying project files..."
cp -r src "$TEMP_DIR/" || { echo "src not found"; exit 3; }
if [[ -d config ]]; then
  cp -r config "$TEMP_DIR/"
else
  echo "Warning: config/ not found; continuing"
fi

# copy requirements if exists
if [[ -f "$REQUIREMENTS_FILE" ]]; then
  cp "$REQUIREMENTS_FILE" "$TEMP_DIR/requirements.txt"
else
  echo "No requirements file found at $REQUIREMENTS_FILE — continuing without third-party wheels"
fi

# Create entry script for Glue (script will be at root of package)
cat > "$TEMP_DIR/glue_entry.py" <<'PY'
import sys, os
from awsglue.utils import getResolvedOptions
import yaml

# Append package src
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# your imports here - ensure these modules exist in src
from utils.logger import setup_logging
from utils.spark_helper import create_glue_context
from transform import clean_data
from writers import write_to_parquet

def load_config_from_s3(s3_path):
    import boto3
    from botocore.exceptions import ClientError
    if not s3_path.startswith("s3://"):
        raise ValueError("config_path must be an s3:// path")
    bucket_key = s3_path.replace("s3://","").split("/",1)
    bucket = bucket_key[0]
    key = bucket_key[1] if len(bucket_key)>1 else ""
    if not key:
        raise ValueError("config_path must include object key (s3://bucket/path/to/config.yaml)")
    s3 = boto3.client('s3')
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        content = resp['Body'].read().decode('utf-8')
        return yaml.safe_load(content)
    except ClientError as e:
        raise RuntimeError(f"Failed to load config from {s3_path}: {e}")

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME','config_path'])
    logger = setup_logging()
    logger.info(f"Starting job {args['JOB_NAME']}")
    glue_context, spark = create_glue_context()
    configs = load_config_from_s3(args['config_path'])
    # validate configs minimal fields
    if 'output' not in configs or 'path' not in configs['output']:
        raise RuntimeError("Missing output.path in config")
    try:
        df = clean_data.run(spark, configs)
        write_to_parquet.write_df_to_s3(df, configs['output']['path'])
        logger.info("Job finished")
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise

if __name__ == '__main__':
    main()
PY

# build wheel files for requirements (optional)
if [[ -f "$TEMP_DIR/requirements.txt" ]]; then
  echo "Attempting to build wheels for requirements (requires pip). This is optional but recommended for Glue."
  mkdir -p "$TEMP_DIR/wheels" || true
  if command -v pip >/dev/null 2>&1; then
    python -m pip wheel -r "$TEMP_DIR/requirements.txt" -w "$TEMP_DIR/wheels" || echo "pip wheel failed; you can still upload requirements.txt"
  else
    echo "pip not available; skipping wheel build"
  fi
fi

# zip package (use absolute paths to make archive predictable)
pushd "$TEMP_DIR" >/dev/null
zip -r "$PACKAGE_NAME" glue_entry.py src $( [[ -d config ]] && echo "config" || echo "") $( [[ -f requirements.txt ]] && echo "requirements.txt" || echo "")
# include wheels dir if exists
if [[ -d wheels && $(ls wheels | wc -l) -gt 0 ]]; then
  zip -r "$PACKAGE_NAME" wheels
fi
popd >/dev/null

# upload artifacts
S3_PACKAGE_PATH="s3://$S3_BUCKET/$S3_PREFIX/$PACKAGE_NAME"
echo "Uploading package to $S3_PACKAGE_PATH"
$AWS_CMD s3 cp "$TEMP_DIR/$PACKAGE_NAME" "$S3_PACKAGE_PATH" "${AWS_CLI_ARGS[@]}"

# upload any wheels to a wheels/ folder
if [[ -d "$TEMP_DIR/wheels" && $(ls "$TEMP_DIR/wheels" | wc -l) -gt 0 ]]; then
  echo "Uploading wheels to s3://$S3_BUCKET/$S3_PREFIX/wheels/"
  $AWS_CMD s3 cp "$TEMP_DIR/wheels/" "s3://$S3_BUCKET/$S3_PREFIX/wheels/" --recursive "${AWS_CLI_ARGS[@]}"
fi

# upload configs if present
if [[ -d config ]]; then
  echo "Uploading configs to s3://$S3_BUCKET/$S3_PREFIX/config/"
  $AWS_CMD s3 cp config/ "s3://$S3_BUCKET/$S3_PREFIX/config/" --recursive "${AWS_CLI_ARGS[@]}"
fi

echo "Done. Package: $S3_PACKAGE_PATH"
echo "Glue job suggestions (what DEs normally set):"
echo " - Script S3 path: $S3_PACKAGE_PATH (or glue_entry.py in that zip)"
echo " - Python library path: s3://$S3_BUCKET/$S3_PREFIX/$PACKAGE_NAME  (for your code)"
echo " - Extra libraries (wheels): s3://$S3_BUCKET/$S3_PREFIX/wheels/ (add to Python library path or extra-py-files)"
echo " - Job parameter: --config_path s3://$S3_BUCKET/$S3_PREFIX/config/config_prod.yaml"
