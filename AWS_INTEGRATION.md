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

## 2. Create Storage Credential (Unity Catalog Required)

### Step 2.1: Create Storage Credential

```sql
-- Create storage credential using the secrets from your secrets scope
CREATE STORAGE CREDENTIAL s3_storage_credential
WITH (
    AWS_ACCESS_KEY_ID = SECRET('s3-secrets', 'aws-access-key-id'),
    AWS_SECRET_ACCESS_KEY = SECRET('s3-secrets', 'aws-secret-access-key')
);

-- Grant permissions to use the storage credential
GRANT CREATE_EXTERNAL_LOCATION ON STORAGE CREDENTIAL s3_storage_credential TO `your-username@company.com`;
```

### Step 2.2: Create External Location

```sql
-- Create external location using the storage credential
CREATE EXTERNAL LOCATION s3_raw_location
URL 's3://databrick-lakehouse/'
WITH (STORAGE_CREDENTIAL s3_storage_credential);

-- Grant permissions on the external location
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION s3_raw_location TO `your-username@company.com`;
```

### Step 2.3: Create Schema with S3 Location

```sql
-- Create schema pointing to S3 location
CREATE SCHEMA IF NOT EXISTS s3_raw
LOCATION 's3://databrick-lakehouse/s3_raw/'
COMMENT 'Schema for raw data stored in S3';

-- Or using the external location (recommended)
CREATE SCHEMA IF NOT EXISTS s3_raw
MANAGED LOCATION 's3_raw_location'
COMMENT 'Schema for raw data stored in S3';
```

## 3. Alternative Method: Direct Configuration (Without Unity Catalog)

### Step 3.1: Configure S3 Access in Cluster/Notebook

```python
# Configure S3 access using secrets directly in your notebook
spark.conf.set("fs.s3a.access.key", dbutils.secrets.get(scope="s3-secrets", key="aws-access-key-id"))
spark.conf.set("fs.s3a.secret.key", dbutils.secrets.get(scope="s3-secrets", key="aws-secret-access-key"))

# Optional: For temporary credentials
# spark.conf.set("fs.s3a.session.token", dbutils.secrets.get(scope="s3-secrets", key="aws-session-token"))

# Set S3 endpoint (adjust region as needed)
spark.conf.set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")
```

### Step 3.2: Create Schema Without Unity Catalog

```sql
-- Create database/schema in Hive metastore
CREATE DATABASE IF NOT EXISTS s3_raw
COMMENT 'Schema for raw data stored in S3';

-- Use the database
USE s3_raw;
```

## 4. Alternative: Instance Profile Method (For AWS)

### Create IAM Role and Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::databrick-lakehouse",
        "arn:aws:s3:::databrick-lakehouse/*"
      ]
    }
  ]
}
```

### Configure Cluster with Instance Profile

```python
# In cluster configuration, add instance profile ARN
# This eliminates the need for storing access keys in secrets
```

## 5. Test the Integration

### Test S3 Access

```python
# Test reading from S3
df = spark.read.option("header", "true").csv("s3://databrick-lakehouse/sample-data/")
df.show()

# Test writing to S3
df.write.mode("overwrite").parquet("s3://databrick-lakehouse/test-output/")

# List files in S3 bucket
dbutils.fs.ls("s3://databrick-lakehouse/")
```

### Create Tables in the Schema

```sql
-- Create a table in the s3_raw schema
CREATE TABLE s3_raw.sample_table (
    id INT,
    name STRING,
    created_date DATE
)
USING DELTA
LOCATION 's3://databrick-lakehouse/s3_raw/sample_table/';

-- Insert sample data
INSERT INTO s3_raw.sample_table VALUES 
(1, 'John Doe', '2025-01-01'),
(2, 'Jane Smith', '2025-01-02');
```

## 6. Best Practices

### Security Considerations

- ✅ Use secrets scope instead of hardcoding credentials
- ✅ Implement least privilege access policies
- ✅ Regularly rotate access keys
- ✅ Use instance profiles when possible on AWS

### Performance Optimization

```python
# Enable S3 optimizations
spark.conf.set("fs.s3a.connection.maximum", "100")
spark.conf.set("fs.s3a.threads.max", "64")
spark.conf.set("fs.s3a.connection.establish.timeout", "5000")
spark.conf.set("fs.s3a.connection.timeout", "200000")
```

### Monitoring and Logging

```python
# Enable S3 request logging
spark.conf.set("fs.s3a.bucket.databrick-lakehouse.request.logging", "true")
```

## Troubleshooting

### Common Issues

1. **Access Denied Errors**
   - Verify IAM permissions
   - Check bucket policy
   - Ensure correct AWS region

2. **Connection Timeouts**
   - Adjust connection timeout settings
   - Check network connectivity
   - Verify S3 endpoint configuration

3. **Authentication Issues**
   - Verify secrets scope configuration
   - Check access key validity
   - Ensure proper credential rotation

4. **Special Characters in Secrets (e.g., dashes)**
   ```bash
   # Error: "No previous regular expression"
   # Solution: Use quotes or interactive mode
   
   # Method 1: Quote the entire secret
   databricks secrets put --scope s3-secrets --key aws-secret-access-key --string-value "secret-with-dashes-123"
   
   # Method 2: Use interactive mode (recommended)
   databricks secrets put --scope s3-secrets --key aws-secret-access-key
   # Then paste your secret when prompted
   
   # Method 3: Use environment variable
   export MY_SECRET="secret-with-dashes-123"
   databricks secrets put --scope s3-secrets --key aws-secret-access-key --string-value "$MY_SECRET"
   
   # Method 4: Use file input
   echo "secret-with-dashes-123" | databricks secrets put --scope s3-secrets --key aws-secret-access-key
   ```

5. **Alternative: Using Databricks UI**
   - Navigate to Databricks workspace
   - Go to Settings → Developer → Secrets
   - Create secret scope manually
   - Add secrets through the web interface (handles special characters automatically)

### Verification Commands

```python
# Verify secrets scope
dbutils.secrets.listScopes()

# Verify S3 connectivity
dbutils.fs.ls("s3://databrick-lakehouse/")

# Check cluster configuration
spark.conf.get("fs.s3a.access.key")  # Should show [REDACTED]
```

## Additional Resources

- [Databricks Documentation](https://docs.databricks.com/)
- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [Unity Catalog Documentation](https://docs.databricks.com/unity-catalog/)

## Support

For issues or questions, please refer to:
- Databricks Support Portal
- AWS Support Center
- Internal team documentation

---

**Note:** Replace `databrick-lakehouse` with your actual S3 bucket name and update configuration values according to your specific environment.