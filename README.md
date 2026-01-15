# Top-produce-ETL

![Build Status](https://github.com/jiazhi110/Top-produce-ETL/actions/workflows/deploy-dev.yml/badge.svg)

## Project Overview

This is a **Production-Grade** distributed data processing pipeline built with **Apache Spark (PySpark)**. It analyzes large-scale user behavior data to identify top-performing products across different regions (Top 3 Products per Region).

The project demonstrates **Modern Data Platform Engineering** best practices:
*   **Cloud-Native Architecture**: Built on AWS Glue (Serverless) and S3.
*   **Robust Data Quality**: Implements **Explicit Schema-on-Read** to prevent schema drift.
*   **Test-Driven Development (TDD)**: Includes comprehensive unit tests (business logic) and integration tests (mock I/O).
*   **CI/CD Pipeline**: Automated testing, packaging, and deployment via GitHub Actions.
*   **Environment Isolation**: Single codebase adaptable to both Local Dev and AWS Glue Prod environments.

## Technology Stack

*   **Compute**: Apache Spark 3.3+ (PySpark)
*   **Storage**: AWS S3 (Delta Lake / Parquet / CSV)
*   **Orchestration**: AWS Glue
*   **Infrastructure as Code**: Terraform (Managed in separate infra repo)
*   **CI/CD**: GitHub Actions
*   **Language**: Python 3.9+

## Project Structure

```bash
.
├── src/                        # Source Code
│   ├── main/
│   │   └── job_runner.py       # Universal Entry Point (Local & Glue)
│   ├── readers/                # I/O Abstraction Layer (S3 Readers)
│   ├── transform/              # Pure Transformation Logic (Testable)
│   ├── writers/                # Data Persistence Layer
│   ├── schemas/                # Explicit Schema Contracts (StructType)
│   └── utils/                  # Helper modules (Logger, Spark Session, Env Detection)
├── config/                     # Configuration Management
│   ├── config_dev.yaml         # Development Config
│   └── config_prod.yaml        # Production Config
├── test/                       # Comprehensive Test Suite
│   ├── test_clean_data.py      # Integration Tests (with local Spark)
│   ├── test_readers.py         # Unit Tests (with Mocking)
│   └── test_spark_helper.py    # Environment Detection Tests
├── .github/workflows/          # Automation Pipelines
├── dev-requirements.txt        # Dev Dependencies (pytest, pandas, etc.)
└── prod-requirements.txt       # Runtime Dependencies (Glue compatible)
```

## Key Features

### 1. Explicit Schema Definition
In production, `inferSchema` is performance-intensive and error-prone. This project defines strict **Data Contracts** in `src/schemas/`, ensuring:
*   **Fail-Fast**: Bad data is caught immediately at the read stage.
*   **High Performance**: Eliminates expensive data scanning overhead.

### 2. Environment Agnostic
`job_runner.py` intelligently detects the execution context:
*   **Local**: Loads local `.yaml` configs and starts a local SparkSession for debugging.
*   **Glue**: Parses AWS Glue arguments (`--config_path`) and initializes GlueContext for cloud execution.

### 3. Testing Strategy
A tiered testing strategy is employed:
*   **Business Logic**: Verified using `pytest` fixtures with a local Spark instance.
*   **I/O Mocking**: S3 interactions are mocked using `unittest.mock` to ensure CI speed and independence from network/credentials.

## Getting Started

### Prerequisites
*   Python 3.9+
*   Java 8 or 11 (Required for local Spark)
*   **Local Development JARs**: To run Spark locally with S3 access, verify `jars/` folder contains:
    *   `hadoop-aws-3.3.2.jar`
    *   `aws-java-sdk-bundle-1.11.1026.jar`
    *   *(Note: These are not tracked in Git. Please download manually if missing.)*

### 1. Installation
```bash
# Clone repository
git clone https://github.com/jiazhi110/Top-produce-ETL.git
cd Top-produce-ETL

# Install dev dependencies
pip install -r dev-requirements.txt
```

### 2. Run Tests (Local)
We use unit tests to verify logic locally instead of running full datasets:
```bash
# Run all tests (Business Logic + Mock I/O + Env Detection)
pytest test/ -v
```

### 3. Deployment (CI/CD)
Manual packaging is discouraged. Deployment is managed by **GitHub Actions**:
1.  **Develop Branch**: Push code -> Triggers `deploy-dev.yml` -> Deploys to AWS Glue Dev.
2.  **Main Branch**: PR Merge -> Triggers Prod deployment.

The pipeline executes: `Test` -> `Package (Zip)` -> `Upload to S3`.

## Maintainer
*   **Author**: Justin
*   **Role**: Data Platform Engineer
