# Databricks S3 Integration Guide

This guide provides detailed steps to integrate Databricks with Amazon S3, including setting up secrets scope and creating schemas with S3 as the warehouse location.

## Prerequisites

- Databricks workspace access
- AWS account with S3 bucket (`databrick-lakehouse` in this example)
- Appropriate IAM permissions for S3 access

## 1. Create Secrets Scope (Using Databricks CLI - Best Practice)

### Install Databricks CLI

```bash
brew install databricks/tap/databricks

# or

uv init --python 3.1
uv venv .venv --python 3.11
uv add databricks-cli
```

### Configure Databricks CLI

```bash
# Configure authentication
databricks configure --token

# You'll be prompted for:
# - Databricks workspace URL (e.g., https://dbc-c2db7cdf-dca0.cloud.databricks.com/)
# - Personal access token
```

### Create Secrets Scope

```bash
# Create a secret scope backed by Databricks
databricks secrets create-scope --scope s3-secrets

# Or create an Azure Key Vault-backed scope (if using Azure)
databricks secrets create-scope --scope s3-secrets --initial-manage-principal users
```

### Add S3 Credentials to Secrets Scope

```bash
# Add AWS Access Key ID & save
databricks secrets put --scope s3-secrets --key aws-access-key-id --string-value "AKIATCQNDK5VHSR6PL5C"

# Add AWS Secret Access Key
databricks secrets put --scope s3-secrets --key aws-secret-access-key --string-value "YOUR_ACCESS_SECRET_HERE"

# Optional: Add AWS Session Token (for temporary credentials)
databricks secrets put --scope s3-secrets --key aws-session-token
```


## Databricks AWS S3 Integration Guide

This guide provides comprehensive instructions for integrating Databricks with AWS S3 using multiple methods.

## 1. Prerequisites

Before starting, ensure you have:
- AWS account with S3 bucket access
- Databricks workspace configured
- AWS credentials (Access Key ID and Secret Access Key)
- Databricks secrets scope configured

## 2. Method 1: Direct S3 Access

### 2.1. Access S3 Data Without Mounting

```python
# Configure S3 access using secrets in your Databricks notebook
spark.conf.set("fs.s3a.access.key", dbutils.secrets.get(scope="s3-secrets", key="aws-access-key-id"))
spark.conf.set("fs.s3a.secret.key", dbutils.secrets.get(scope="s3-secrets", key="aws-secret-access-key"))

# Set S3 endpoint (adjust region as needed)
spark.conf.set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")

# Read data from S3
df = spark.read.csv('s3://movie-dataset-youtube/movie_statistic_dataset.csv', inferSchema=True, header=True)
display(df)
```

### 2.2. Access S3 Data by Mounting the Bucket

```python
# Get credentials from secrets
access_key = dbutils.secrets.get(scope="s3-secrets", key="aws-access-key-id")
secret_key = dbutils.secrets.get(scope="s3-secrets", key="aws-secret-access-key")
encoded_secret_key = secret_key.replace("/", "%2F")

# Configure Spark for S3 access
spark.conf.set("fs.s3a.access.key", access_key)
spark.conf.set("fs.s3a.secret.key", secret_key)

# Mount S3 bucket
aws_bucket_name = "movie-dataset-youtube"
mount_name = "s3data"

dbutils.fs.mount(
    f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", 
    f"/mnt/{mount_name}"
)

# List files in mounted location
display(dbutils.fs.ls(f"/mnt/{mount_name}"))

# Unmount when done
dbutils.fs.unmount(f"/mnt/{mount_name}")
```

## 3. Method 2: Unity Catalog Integration
This method uses Unity Catalog to manage S3 data access and schemas. We need to create an external location and schema in Unity Catalog.

### 3.1. Create External Location in Unity Catalog using Cloud Formation

![cloudformation.png](docs/etc/cloudformation.png)

![cloudformation_2.png](docs/etc/cloudformation_2.png)

![cloudformation_2.png](docs/etc/cloudformation_3.png)

### 3.2. Using Storage Credentials & Profile instance
https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage#manage-storage-credentials

https://www.youtube.com/watch?v=cylJ9hPmt7c

### 3.1. Integrate S3 with Unity Catalog

- Create the IAM role with a Custom Trust Policy.
- In the Custom Trust Policy field, paste the following policy JSON.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": ["arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"]
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<DATABRICKS_ACCOUNT_ID>" 
        }
      }
    }
  ]
}
```

## Create Catalog from external location

- External Location Name: db_s3_external_databricks-s3-ingest-806e3
- S3 Path: s3://katolakehouse/

### Method 1: Create New Catalog (Recommended for Production)
![new_catalog.png](docs/etc/new_catalog.png)

![lakehouse.png](docs/etc/lakehouse.png)

Use catalog lakehouse
```sql


```