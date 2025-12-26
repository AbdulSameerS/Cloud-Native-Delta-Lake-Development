# Cloud-Native Delta Lake Development

This project implements a scalable, cloud-based Delta Lake pipeline using **AWS Glue**, **Amazon S3**, and **Amazon Redshift**. It processes structured and unstructured data, ensuring ACID compliance via Delta Lake logs.

## Architecture

1.  **Ingestion (Bronze)**: Raw data (CSV/JSON) is uploaded to the S3 Raw Bucket.
2.  **Processing (Silver)**: AWS Glue ETL Jobs (PySpark) read raw data, clean it, and write it to the S3 Silver Bucket in **Delta Format**. This provides ACID transactions and schema enforcement.
3.  **Analytics (Gold)**: Aggregated business metrics are computed and stored. These can be queried via **Amazon Redshift Spectrum** or loaded into Redshift tables.

![Architecture Diagram](https://upload.wikimedia.org/wikipedia/commons/thumb/b/b9/Solid_white_drawing_pin.svg/1200px-Solid_white_drawing_pin.svg.png) *(Placeholder for your diagram)*

## Project Structure

```
├── data/                   # Local sample data (generated)
├── infrastructure/
│   └── terraform/          # Infrastructure as Code (AWS Resources)
├── src/
│   ├── jobs/               # AWS Glue PySpark Scripts
│   ├── scripts/            # Local utility scripts
│   └── redshift/           # data warehouse SQL
└── requirements.txt        # Local dependencies
```

## Setup & Deployment

### 1. Generate Data
Run the local script to generate synthetic Orders (Structured) and Reviews (Unstructured) data.
```bash
python3 src/scripts/generate_data.py
```

### 2. Infrastructure (Terraform)
Deploy the AWS resources.
```bash
cd infrastructure/terraform
terraform init
terraform apply
```
*Note: Ensure you have AWS credentials configured.*

### 3. Deploy Glue Jobs
Sync the `src/jobs` script to the S3 bucket created by Terraform.
```bash
aws s3 cp src/jobs/process_delta_lake.py s3://<YOUR_RAW_BUCKET_NAME>/scripts/
```

### 4. Run the Pipeline
Trigger the Glue Job via AWS Console or CLI.
```bash
aws glue start-job-run --job-name cloud-native-delta-etl-job
```

## Local Development
To test the PySpark logic locally, install dependencies:
```bash
pip install -r requirements.txt
```
(Note: The provided Glue script uses `awsglue` libraries which are specific to the AWS environment. Examples for local logic adaptation are in the source.)
