# Databricks Role-Based Access Control (RBAC) Implementation Guide

## Table of Contents
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Unity Catalog Setup](#unity-catalog-setup)
- [User and Group Management](#user-and-group-management)
- [Role Definitions](#role-definitions)
- [Permission Structure](#permission-structure)
- [Implementation Steps](#implementation-steps)
- [Data Access Patterns](#data-access-patterns)
- [Security Best Practices](#security-best-practices)
- [Monitoring and Auditing](#monitoring-and-auditing)
- [Troubleshooting](#troubleshooting)
- [Common Use Cases](#common-use-cases)

## Overview

This guide provides comprehensive instructions for implementing Role-Based Access Control (RBAC) in Databricks using Unity Catalog. RBAC ensures that users have appropriate access to data and resources based on their roles and responsibilities.

### Key Components
- **Unity Catalog**: Centralized governance and security
- **Groups**: Collections of users with similar access needs
- **Privileges**: Specific permissions on objects
- **Policies**: Data masking and filtering rules
- **External Locations**: Secure cloud storage access

## Prerequisites

Before implementing RBAC, ensure you have:

- [ ] Databricks Premium or Enterprise workspace
- [ ] Unity Catalog enabled
- [ ] Account admin privileges
- [ ] External storage locations configured (S3, Azure, GCS)
- [ ] Service principals set up for automation

## Unity Catalog Setup

### 1. Enable Unity Catalog
```sql
-- Verify Unity Catalog is enabled
SELECT current_catalog();

-- List available catalogs
SHOW CATALOGS;
```

### 2. Create Catalog Structure
```sql
-- Create main catalog
CREATE CATALOG IF NOT EXISTS lakehouse
COMMENT 'Main data lakehouse catalog';

-- Create schemas for different data layers
CREATE SCHEMA IF NOT EXISTS lakehouse.raw
MANAGED LOCATION 's3://your-bucket/raw'
COMMENT 'Bronze layer - raw ingested data';

CREATE SCHEMA IF NOT EXISTS lakehouse.refined
MANAGED LOCATION 's3://your-bucket/refined'
COMMENT 'Silver layer - cleaned and transformed data';

CREATE SCHEMA IF NOT EXISTS lakehouse.enterprise
MANAGED LOCATION 's3://your-bucket/enterprise'
COMMENT 'Gold layer - business-ready data';
```

## User and Group Management

### 1. Create Groups
```sql
-- Create groups for different roles
CREATE GROUP IF NOT EXISTS data_engineers
COMMENT 'Data engineers with full pipeline access';

CREATE GROUP IF NOT EXISTS data_analysts
COMMENT 'Analysts with read access to refined data';

CREATE GROUP IF NOT EXISTS data_scientists
COMMENT 'Scientists with advanced analytics access';

CREATE GROUP IF NOT EXISTS business_users
COMMENT 'Business users with dashboard access';

CREATE GROUP IF NOT EXISTS admin_users
COMMENT 'Administrative users with full access';
```

### 2. Add Users to Groups
```sql
-- Add users to appropriate groups
ALTER GROUP data_engineers ADD USER 'engineer1@company.com';
ALTER GROUP data_engineers ADD USER 'engineer2@company.com';

ALTER GROUP data_analysts ADD USER 'analyst1@company.com';
ALTER GROUP data_analysts ADD USER 'analyst2@company.com';

ALTER GROUP data_scientists ADD USER 'scientist1@company.com';
ALTER GROUP data_scientists ADD USER 'scientist2@company.com';

ALTER GROUP business_users ADD USER 'business1@company.com';
ALTER GROUP business_users ADD USER 'business2@company.com';

ALTER GROUP admin_users ADD USER 'admin@company.com';
```

### 3. Verify Group Membership
```sql
-- Check group membership
SHOW GROUPS;
DESCRIBE GROUP data_engineers;
```

## Role Definitions

### Data Engineer Role
- **Responsibilities**: Data pipeline development, ETL processes
- **Access**: Full access to raw and refined layers, limited enterprise access
- **Privileges**: CREATE, SELECT, INSERT, UPDATE, DELETE on raw/refined

### Data Analyst Role
- **Responsibilities**: Data analysis, reporting, dashboards
- **Access**: Read access to refined and enterprise layers
- **Privileges**: SELECT on refined/enterprise, masked sensitive data

### Data Scientist Role
- **Responsibilities**: ML model development, advanced analytics
- **Access**: Read access to all layers, ML workspace access
- **Privileges**: SELECT on all layers, CREATE ML models

### Business User Role
- **Responsibilities**: Business reporting, dashboard consumption
- **Access**: Read access to enterprise layer only
- **Privileges**: SELECT on enterprise with heavy masking

### Admin Role
- **Responsibilities**: System administration, security management
- **Access**: Full access to all resources
- **Privileges**: ALL privileges on all objects

## Permission Structure

### Catalog Level Permissions
```sql
-- Grant catalog usage
GRANT USE CATALOG ON CATALOG lakehouse TO data_engineers;
GRANT USE CATALOG ON CATALOG lakehouse TO data_analysts;
GRANT USE CATALOG ON CATALOG lakehouse TO data_scientists;
GRANT USE CATALOG ON CATALOG lakehouse TO business_users;
GRANT ALL PRIVILEGES ON CATALOG lakehouse TO admin_users;
```

### Schema Level Permissions
```sql
-- Raw schema permissions (Bronze layer)
GRANT USE SCHEMA ON SCHEMA lakehouse.raw TO data_engineers;
GRANT ALL PRIVILEGES ON SCHEMA lakehouse.raw TO data_engineers;
GRANT SELECT ON SCHEMA lakehouse.raw TO data_scientists;

-- Refined schema permissions (Silver layer)
GRANT USE SCHEMA ON SCHEMA lakehouse.refined TO data_engineers;
GRANT ALL PRIVILEGES ON SCHEMA lakehouse.refined TO data_engineers;
GRANT USE SCHEMA ON SCHEMA lakehouse.refined TO data_analysts;
GRANT SELECT ON SCHEMA lakehouse.refined TO data_analysts;
GRANT SELECT ON SCHEMA lakehouse.refined TO data_scientists;

-- Enterprise schema permissions (Gold layer)
GRANT USE SCHEMA ON SCHEMA lakehouse.enterprise TO data_engineers;
GRANT SELECT, INSERT ON SCHEMA lakehouse.enterprise TO data_engineers;
GRANT USE SCHEMA ON SCHEMA lakehouse.enterprise TO data_analysts;
GRANT SELECT ON SCHEMA lakehouse.enterprise TO data_analysts;
GRANT USE SCHEMA ON SCHEMA lakehouse.enterprise TO data_scientists;
GRANT SELECT ON SCHEMA lakehouse.enterprise TO data_scientists;
GRANT USE SCHEMA ON SCHEMA lakehouse.enterprise TO business_users;
GRANT SELECT ON SCHEMA lakehouse.enterprise TO business_users;
```

### Table Level Permissions
```sql
-- Grant specific table permissions
GRANT SELECT ON TABLE lakehouse.enterprise.customer_data TO data_analysts;
GRANT SELECT ON TABLE lakehouse.enterprise.sales_summary TO business_users;
GRANT ALL PRIVILEGES ON TABLE lakehouse.raw.source_data TO data_engineers;
```

## Implementation Steps

### Step 1: Assessment and Planning
1. **Identify User Roles**: Map organizational roles to data access needs
2. **Define Data Classification**: Classify data by sensitivity level
3. **Create Access Matrix**: Document who needs access to what

### Step 2: Group Creation and User Assignment
```sql
-- Create groups based on roles
CREATE GROUP IF NOT EXISTS finance_team;
CREATE GROUP IF NOT EXISTS marketing_team;
CREATE GROUP IF NOT EXISTS operations_team;

-- Add users to domain-specific groups
ALTER GROUP finance_team ADD USER 'finance1@company.com';
ALTER GROUP marketing_team ADD USER 'marketing1@company.com';
ALTER GROUP operations_team ADD USER 'ops1@company.com';
```

### Step 3: Hierarchical Permissions
```sql
-- Create nested groups for hierarchical access
CREATE GROUP IF NOT EXISTS senior_analysts;
CREATE GROUP IF NOT EXISTS junior_analysts;

-- Add groups to other groups
ALTER GROUP senior_analysts ADD GROUP junior_analysts;
ALTER GROUP data_analysts ADD GROUP senior_analysts;
```

### Step 4: External Location Setup
```sql
-- Create external locations for different environments
CREATE EXTERNAL LOCATION IF NOT EXISTS prod_data_location
URL 's3://production-bucket/data/'
WITH (CREDENTIAL `production-credential`)
COMMENT 'Production data storage';

CREATE EXTERNAL LOCATION IF NOT EXISTS dev_data_location
URL 's3://development-bucket/data/'
WITH (CREDENTIAL `development-credential`)
COMMENT 'Development data storage';

-- Grant access to external locations
GRANT READ FILES ON EXTERNAL LOCATION prod_data_location TO data_engineers;
GRANT READ FILES ON EXTERNAL LOCATION dev_data_location TO data_engineers;
```

### Step 5: Data Masking Implementation
```sql
-- Create masking functions
CREATE OR REPLACE FUNCTION lakehouse.enterprise.mask_email(email STRING)
RETURNS STRING
LANGUAGE SQL
DETERMINISTIC
RETURN 
  CASE 
    WHEN is_account_group_member('admin_users') THEN email
    WHEN is_account_group_member('data_analysts') THEN email
    ELSE CONCAT(LEFT(email, 2), '***@***.com')
  END;

-- Apply masking to sensitive columns
ALTER TABLE lakehouse.enterprise.customer_data 
ALTER COLUMN email 
SET MASK lakehouse.enterprise.mask_email;
```

### Step 6: Row-Level Security
```sql
-- Create row filter function
CREATE OR REPLACE FUNCTION lakehouse.enterprise.filter_customer_data()
RETURNS BOOLEAN
LANGUAGE SQL
DETERMINISTIC
RETURN 
  CASE 
    WHEN is_account_group_member('admin_users') THEN TRUE
    WHEN is_account_group_member('finance_team') AND region = 'US' THEN TRUE
    WHEN is_account_group_member('marketing_team') AND segment = 'Premium' THEN TRUE
    ELSE FALSE
  END;

-- Apply row filter
ALTER TABLE lakehouse.enterprise.customer_data 
SET ROW FILTER lakehouse.enterprise.filter_customer_data ON ();
```

## Data Access Patterns

### Pattern 1: Layered Access (Medallion Architecture)
```sql
-- Bronze layer - Raw data access
-- Only data engineers can write, scientists can read
GRANT ALL PRIVILEGES ON SCHEMA lakehouse.raw TO data_engineers;
GRANT SELECT ON SCHEMA lakehouse.raw TO data_scientists;

-- Silver layer - Refined data access
-- Engineers can write, analysts and scientists can read
GRANT ALL PRIVILEGES ON SCHEMA lakehouse.refined TO data_engineers;
GRANT SELECT ON SCHEMA lakehouse.refined TO data_analysts;
GRANT SELECT ON SCHEMA lakehouse.refined TO data_scientists;

-- Gold layer - Business data access
-- Limited write access, broad read access with masking
GRANT SELECT, INSERT ON SCHEMA lakehouse.enterprise TO data_engineers;
GRANT SELECT ON SCHEMA lakehouse.enterprise TO data_analysts;
GRANT SELECT ON SCHEMA lakehouse.enterprise TO business_users;
```

### Pattern 2: Domain-Based Access
```sql
-- Financial data access
CREATE SCHEMA IF NOT EXISTS lakehouse.finance;
GRANT ALL PRIVILEGES ON SCHEMA lakehouse.finance TO finance_team;
GRANT SELECT ON SCHEMA lakehouse.finance TO admin_users;

-- Marketing data access
CREATE SCHEMA IF NOT EXISTS lakehouse.marketing;
GRANT ALL PRIVILEGES ON SCHEMA lakehouse.marketing TO marketing_team;
GRANT SELECT ON SCHEMA lakehouse.marketing TO admin_users;
```

### Pattern 3: Time-Based Access
```sql
-- Create function for time-based access
CREATE OR REPLACE FUNCTION lakehouse.enterprise.time_based_filter()
RETURNS BOOLEAN
LANGUAGE SQL
DETERMINISTIC
RETURN 
  CASE 
    WHEN is_account_group_member('admin_users') THEN TRUE
    WHEN is_account_group_member('data_analysts') AND 
         created_date >= DATE_SUB(CURRENT_DATE(), 90) THEN TRUE
    ELSE FALSE
  END;
```

## Security Best Practices

### 1. Principle of Least Privilege
- Grant minimum permissions required for job function
- Regularly review and audit permissions
- Use time-limited access where appropriate

### 2. Group-Based Management
- Always use groups instead of individual user permissions
- Create logical group hierarchies
- Document group purposes and membership criteria

### 3. Sensitive Data Protection
```sql
-- Create tags for sensitive data
CREATE TAG IF NOT EXISTS lakehouse.enterprise.PII;
CREATE TAG IF NOT EXISTS lakehouse.enterprise.CONFIDENTIAL;

-- Apply tags to sensitive columns
ALTER TABLE lakehouse.enterprise.customer_data 
ALTER COLUMN ssn 
SET TAGS ('lakehouse.enterprise.PII' = 'social_security');

ALTER TABLE lakehouse.enterprise.customer_data 
ALTER COLUMN credit_score 
SET TAGS ('lakehouse.enterprise.CONFIDENTIAL' = 'financial');
```

### 4. Audit and Monitoring
```sql
-- Query audit logs
SELECT 
  event_time,
  user_identity,
  action_name,
  request_params
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND event_time >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY event_time DESC;
```

## Monitoring and Auditing

### 1. Permission Auditing
```sql
-- Check effective permissions for a user
SHOW GRANTS ON TABLE lakehouse.enterprise.customer_data;

-- List all permissions for a schema
SHOW GRANTS ON SCHEMA lakehouse.enterprise;

-- Check group memberships
SHOW GROUPS;
```

### 2. Access Monitoring
```sql
-- Monitor table access patterns
SELECT 
  table_name,
  user_identity,
  COUNT(*) as access_count
FROM system.access.table_access
WHERE catalog_name = 'lakehouse'
  AND access_date >= current_date() - 7
GROUP BY table_name, user_identity
ORDER BY access_count DESC;
```

### 3. Failed Access Attempts
```sql
-- Monitor failed access attempts
SELECT 
  event_time,
  user_identity,
  action_name,
  request_params,
  response.error_code
FROM system.access.audit
WHERE response.status_code != 200
  AND event_time >= current_timestamp() - INTERVAL 24 HOURS;
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Permission Denied Errors
```sql
-- Check current user permissions
SELECT current_user();
SHOW GRANTS FOR current_user();

-- Check effective permissions on specific object
SHOW GRANTS ON TABLE lakehouse.enterprise.customer_data;
```

#### 2. Group Membership Issues
```sql
-- Verify group membership
SELECT 
  group_name,
  principal_name,
  principal_type
FROM system.information_schema.groups
WHERE group_name = 'data_analysts';
```

#### 3. Masking Function Errors
```sql
-- Test masking function
SELECT lakehouse.enterprise.mask_email('test@example.com');

-- Check if user is in expected group
SELECT is_account_group_member('data_analysts');
```

#### 4. External Location Access Issues
```sql
-- Check external location configuration
DESCRIBE EXTERNAL LOCATION prod_data_location;

-- Verify credentials
SHOW EXTERNAL LOCATIONS;
```

## Common Use Cases

### Use Case 1: Multi-Tenant SaaS Platform
```sql
-- Create tenant-specific schemas
CREATE SCHEMA IF NOT EXISTS lakehouse.tenant_a;
CREATE SCHEMA IF NOT EXISTS lakehouse.tenant_b;

-- Create tenant-specific groups
CREATE GROUP IF NOT EXISTS tenant_a_users;
CREATE GROUP IF NOT EXISTS tenant_b_users;

-- Grant tenant-specific access
GRANT ALL PRIVILEGES ON SCHEMA lakehouse.tenant_a TO tenant_a_users;
GRANT ALL PRIVILEGES ON SCHEMA lakehouse.tenant_b TO tenant_b_users;
```

### Use Case 2: Regulatory Compliance (GDPR, HIPAA)
```sql
-- Create compliance-specific masking
CREATE OR REPLACE FUNCTION lakehouse.enterprise.gdpr_mask_pii(pii_data STRING)
RETURNS STRING
LANGUAGE SQL
DETERMINISTIC
RETURN 
  CASE 
    WHEN is_account_group_member('gdpr_officers') THEN pii_data
    WHEN is_account_group_member('authorized_processors') THEN pii_data
    ELSE 'GDPR_MASKED'
  END;

-- Apply to PII columns
ALTER TABLE lakehouse.enterprise.customer_data 
ALTER COLUMN personal_info 
SET MASK lakehouse.enterprise.gdpr_mask_pii;
```

### Use Case 3: Development/Staging/Production Environments
```sql
-- Create environment-specific catalogs
CREATE CATALOG IF NOT EXISTS dev_lakehouse;
CREATE CATALOG IF NOT EXISTS staging_lakehouse;
CREATE CATALOG IF NOT EXISTS prod_lakehouse;

-- Create environment-specific groups
CREATE GROUP IF NOT EXISTS dev_users;
CREATE GROUP IF NOT EXISTS staging_users;
CREATE GROUP IF NOT EXISTS prod_users;

-- Grant environment-specific access
GRANT ALL PRIVILEGES ON CATALOG dev_lakehouse TO dev_users;
GRANT SELECT ON CATALOG staging_lakehouse TO staging_users;
GRANT SELECT ON CATALOG prod_lakehouse TO prod_users;
```

---

## Quick Reference Commands

### Group Management
```sql
-- Create group
CREATE GROUP group_name;

-- Add user to group
ALTER GROUP group_name ADD USER 'user@email.com';

-- Remove user from group
ALTER GROUP group_name DROP USER 'user@email.com';

-- Delete group
DROP GROUP group_name;
```

### Permission Management
```sql
-- Grant permissions
GRANT privilege ON object TO principal;

-- Revoke permissions
REVOKE privilege ON object FROM principal;

-- Show grants
SHOW GRANTS ON object;
```

### Masking and Filtering
```sql
-- Create masking function
CREATE FUNCTION mask_function(column_value TYPE) RETURNS TYPE ...;

-- Apply column mask
ALTER TABLE table_name ALTER COLUMN column_name SET MASK mask_function;

-- Apply row filter
ALTER TABLE table_name SET ROW FILTER filter_function ON ();
```

### Monitoring
```sql
-- Check audit logs
SELECT * FROM system.access.audit WHERE ...;

-- Monitor table access
SELECT * FROM system.access.table_access WHERE ...;

-- Check permissions
SHOW GRANTS ON SCHEMA schema_name;
```

---

## Support and Resources

- **Databricks Documentation**: [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- **Security Best Practices**: [Databricks Security Guide](https://docs.databricks.com/security/index.html)
- **Community**: [Databricks Community Forum](https://community.databricks.com/)

---

*Last Updated: July 2025*
*Version: 1.0*