# Serverless Scalable Data Lake ETL on AWS

## Overview

This project focuses on building a modern, cost-effective, and highly scalable data lake solution entirely using AWS serverless services. It demonstrates an end-to-end Extract, Transform, Load (ETL) pipeline for various data sources (e.g., CSV, JSON files uploaded to S3, external APIs), preparing them for analytical consumption without the need to manage servers.

The architecture emphasizes automation, scalability, and robust data governance, making it ideal for handling growing volumes and varieties of data.

## Problem Solved

Many organizations struggle with managing large, diverse datasets and building traditional ETL pipelines that are often costly, complex to maintain, and difficult to scale. This project addresses these challenges by:
* Providing a **serverless and cost-optimized** data ingestion and transformation framework.
* Enabling **flexible storage** for structured, semi-structured, and unstructured data in a data lake (S3).
* Automating **data cataloging and schema inference**, making data easily discoverable.
* Facilitating **secure and efficient analytical querying** through managed services.
* Reducing operational overhead by eliminating server management.

## High-Level Architecture
``graph TD
    A[Data Sources] --> B(S3 Raw Data Lake);
    B --> C{Trigger: Lambda/EventBridge};
    C --> D[AWS Glue ETL];
    D --> E(S3 Processed Data Lake);
    E --> F[AWS Glue Data Catalog];
    F --> G[AWS Athena];
    G --> H[BI Tool (e.g., QuickSight, Tableau)];
    subgraph Governance
        I[AWS Lake Formation]
    end
    I -- manages access --> B;
    I -- manages access --> E;
    I -- manages access --> F;``

## Technologies Used

* **Cloud Storage:**
    * **AWS S3:** Primary storage for raw and processed data (Data Lake).
* **Serverless ETL/Compute:**
    * **AWS Glue:** Managed ETL service for Spark-based transformations and data cataloging.
    * **AWS Lambda:** Serverless compute for event-driven data ingestion, triggering Glue jobs, or small transformations.
* **Querying:**
    * **AWS Athena:** Serverless interactive query service for S3 data, using the Glue Data Catalog.
* **Data Governance & Security:**
    * **AWS Lake Formation:** For centralized data access control and security management over the data lake.
* **Identity & Access Management:**
    * **AWS IAM:** Managing permissions for all AWS services involved.
* **Orchestration/Workflow:**
    * **AWS Step Functions:** (Optional, for complex multi-step workflows).
    * **CloudWatch Events/EventBridge:** For triggering Lambda/Glue jobs based on S3 events or schedules.
* **Infrastructure as Code:**
    * **AWS CloudFormation / Terraform:** (Highly Recommended) For defining and deploying the entire AWS infrastructure.
* **Programming Language:**
    * **Python:** For Lambda functions and Glue ETL scripts (PySpark).

## Project Phases & Implementation Details

### Phase 1: Data Ingestion & Raw Data Lake
* **Objective:** Set up a landing zone for raw data and implement initial ingestion mechanisms.
* **Implementation:**
    * Created **AWS S3 buckets** for raw data (landing zone) and processed data.
    * Configured **S3 event notifications** to trigger AWS Lambda functions upon new file uploads (e.g., CSV, JSON files).
    * Developed **AWS Lambda functions** (Python) to:
        * Receive S3 events and perform basic validation.
        * Copy raw data to a versioned raw data lake bucket (`s3://your-data-lake/raw/`).
        * Trigger subsequent AWS Glue jobs for transformation.

### Phase 2: Serverless ETL & Processed Data Lake
* **Objective:** Transform raw data into clean, structured, and query-optimized formats within the data lake.
* **Implementation:**
    * Developed **AWS Glue ETL jobs** (PySpark) to read data from the raw S3 bucket.
    * Performed **data cleansing and transformations:**
        * Handling missing values, duplicate removal.
        * Type conversions and schema enforcement.
        * Flattening complex JSON structures.
        * Enriching data with lookups.
    * Wrote processed data back to S3 in a columnar format (e.g., **Parquet**), partitioned by relevant keys (e.g., year/month/day), into a processed data lake bucket (`s3://your-data-lake/processed/`).
    * (Optional) Implemented **data quality checks** within Glue jobs.

### Phase 3: Data Cataloging & Querying
* **Objective:** Make the processed data easily discoverable and queryable by analytical tools.
* **Implementation:**
    * Utilized **AWS Glue Data Catalog** to automatically infer schemas from the Parquet files in the processed S3 bucket.
    * Created **Glue Crawlers** to automatically discover new partitions and schema changes.
    * Demonstrated **AWS Athena** integration, allowing users to perform ad-hoc SQL queries directly on the S3 data lake through the Glue Data Catalog.
    * Configured **Athena Workgroups** for query isolation and cost management.

### Phase 4: Data Governance & Security (AWS Lake Formation)
* **Objective:** Implement centralized access control and security over the data lake.
* **Implementation:**
    * Registered S3 data lake locations with **AWS Lake Formation**.
    * Configured **fine-grained access controls** at the table, column, and row level using Lake Formation permissions.
    * Demonstrated how different IAM roles or users can be granted specific access to subsets of the data.
    * Ensured data encryption at rest (S3 SSE-KMS) and in transit.

## How to Run This Project (AWS Deployment)

### Prerequisites
* An active AWS Account
* AWS CLI configured with programmatic access keys
* Python 3.x
* `boto3` (Python SDK for AWS)
* Basic understanding of S3, Lambda, Glue, Athena, and IAM.
* (Recommended) Terraform or AWS CloudFormation for Infrastructure as Code.

### Deployment Steps (Outline - using AWS CLI/Console, or CloudFormation/Terraform)

1.  **S3 Bucket Setup:**
    * Create two S3 buckets: one for raw data (e.g., `your-data-lake-raw-subash`) and one for processed data (e.g., `your-data-lake-processed-subash`).
2.  **IAM Roles & Policies:**
    * Create IAM roles for AWS Lambda and AWS Glue with necessary permissions (S3 read/write, Glue execution, Athena, Lake Formation).
3.  **Lambda Function for Ingestion Trigger:**
    * Deploy a Python Lambda function triggered by S3 `PutObject` events in the raw bucket. This Lambda could simply log the event or trigger a Glue job.
4.  **AWS Glue ETL Job:**
    * Upload your PySpark ETL script (`glue_transform_script.py`) to an S3 location (e.g., `s3://your-glue-scripts-subash/`).
    * Create an AWS Glue Job, pointing to your script and specifying the IAM role. Configure input/output paths to S3 buckets.
5.  **AWS Glue Crawler:**
    * Create a Glue Crawler pointing to your processed S3 bucket. Run it to populate the Glue Data Catalog.
6.  **AWS Lake Formation Setup:**
    * Register your S3 data lake locations with Lake Formation.
    * Grant permissions to your IAM users/roles via Lake Formation console for specific databases/tables.
7.  **Test Ingestion:**
    * Upload sample CSV/JSON files to your raw S3 bucket.
    * Monitor Lambda logs, Glue job runs, and check for processed data in the processed S3 bucket.
8.  **Query with Athena:**
    * Go to the Athena console and query your processed data using standard SQL.

## Future Enhancements
* Integrate **AWS Step Functions** for complex, multi-stage ETL workflows.
* Implement advanced **data quality checks** using AWS Deequ (with Glue).
* Add **data masking or tokenization** for sensitive data using Lambda or Glue transformations.
* Set up **CloudWatch Dashboards** for end-to-end pipeline monitoring.
* Connect the processed data to **Amazon QuickSight** or another BI tool for interactive dashboards.
* Expand data sources to include databases (e.g., RDS) using Glue connectors.

## Contact
Feel free to reach out with any questions or feedback!
* **Subash Yadav:** [yadavsubash0123@gmail.com](mailto:yadavsubash0123@gmail.com)
* **LinkedIn:** [https://www.linkedin.com/in/mathachew7/]
* **GitHub:** [github.com/mathachew7]

---
**Push to GitHub is still remaining!**
