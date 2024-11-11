## Serverless ETL Workflow with AWS Step Functions for Data Processing and Analytics

### Project Overview
This project demonstrates a **Serverless ETL Workflow** using **AWS Step Functions**, integrating **AWS Lambda**, **AWS IoT Core**, **Amazon S3**, **Amazon SQS**, **pgAdmin (PostgreSQL Database)**, and **CloudWatch**. It provides a scalable solution for extracting, transforming, and loading (ETL) IoT telemetry data into a PostgreSQL database.

### Prerequisites
- **AWS Account** with access to:
  - Amazon S3
  - AWS Lambda
  - AWS Step Functions
  - AWS IoT Core
  - Amazon SQS
  - CloudWatch
- **IAM Roles** with permissions for:
  - S3 access (`AmazonS3FullAccess`)
  - IoT Core access (`AWSIoTFullAccess`)
  - SQS access (`AmazonSQSFullAccess`)
  - Lambda execution (`AWSLambdaBasicExecutionRole`)
- **pgAdmin** setup with a PostgreSQL database instance

### Project Architecture
1. **AWS IoT Core**: Receives telemetry data from IoT devices and forwards it to Amazon SQS.
2. **Amazon SQS**: Stores messages temporarily and triggers the ETL process.
3. **AWS Lambda**: Processes the data, performs transformations, and loads it into the PostgreSQL database.
4. **AWS Step Functions**: Orchestrates the ETL workflow.
5. **Amazon S3**: Stores raw and processed data files.
6. **CloudWatch**: Monitors logs and sends notifications for error handling.

### Steps to Set Up and Execute

#### Step 1: Create an S3 Bucket for Data Storage
1. Open the **Amazon S3 console**.
2. Click on **Create Bucket**, and name it (e.g., `my-etl-bucket`).
3. Configure the bucket with necessary permissions.

#### Step 2: Set Up AWS IoT Core for Data Ingestion
1. **Create an IoT Thing** in the IoT Core console.
2. **Attach a Policy** to allow data publishing.
3. Configure **MQTT Topics** for sending telemetry data.

#### Step 3: Set Up Amazon SQS for Data Queuing
1. Go to the **Amazon SQS** console and create a **Standard Queue** (e.g., `etl-data-queue`).
2. Set the necessary IAM policies to allow Lambda functions to read from the queue.

#### Step 4: Create AWS Lambda Functions
1. **Data Extraction Function**:
   - Go to the **AWS Lambda** console and create a function named `DataExtractionFunction`.
   - Write code to read messages from the SQS queue.
2. **Data Transformation Function**:
   - Create a second function named `DataTransformationFunction`.
   - This function should process and transform the data as required.
3. **Data Loading Function**:
   - Create a third function named `DataLoadingFunction`.
   - This function inserts the transformed data into the PostgreSQL database using `pgAdmin`.

#### Step 5: Set Up PostgreSQL Database in pgAdmin
1. Create a PostgreSQL database in pgAdmin to store telemetry data.
2. Configure database access for the Lambda function.

#### Step 6: Create AWS Step Functions Workflow
1. Go to the **AWS Step Functions** console and create a new **State Machine**.
2. Define the workflow with the following states:
   - **Extract Data** (calls `DataExtractionFunction`)
   - **Transform Data** (calls `DataTransformationFunction`)
   - **Load Data** (calls `DataLoadingFunction`)
3. Set up **error handling** and **retries** for each state.

#### Step 7: Test the ETL Pipeline
1. Send sample telemetry data to IoT Core using a Python script or IoT simulator.
2. Monitor the execution in the **Step Functions** console.
3. Verify that the data is processed and loaded into the PostgreSQL database.

#### Step 8: Monitor and Set Up Alerts with CloudWatch
1. Enable **CloudWatch Logs** for each Lambda function to monitor errors and execution details.
2. Create **CloudWatch Alarms** for error notifications and link them to an SNS topic for alerts.

### Future Enhancements
- **Data Validation**: Add Lambda functions to perform data quality checks.
- **Analytics Integration**: Connect to an analytics service (e.g., Amazon QuickSight) for real-time data visualization.
- **Security Improvements**: Implement additional security features like encryption at rest and in transit.

This setup provides a robust, scalable, and serverless ETL pipeline using AWS services, tailored for IoT data processing and real-time analytics.
