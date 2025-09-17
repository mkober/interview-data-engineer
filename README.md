# Technical Interview - Data Pipeline Code Review

## Overview

This codebase contains AWS data pipeline components for processing admission applications from a Data Lake platform to a university CMS system. The candidate should be prepared to walk through the code and explain its functionality, architecture, and desiggitn decisions.

## Interview Format

- You will be asked to explain different parts of the codebase
- Focus on understanding the data flow, transformations, and AWS services used
- No coding or modifications will be required
- Be prepared to discuss design patterns and architectural decisions

## Key Questions to Prepare For

### 1. Architecture & Data Flow

- **Q: Can you walk through the overall data flow from source to destination?**

  - Where does the data originate?
  - What transformations occur?
  - Where does the processed data end up?

- **Q: What AWS services are being utilized in this pipeline?**
  - Identify each service and its purpose
  - Explain how they interact with each other

### 2. Transformation Layer (`data-transformer/`)

#### Factory Pattern Implementation

- **Q: Explain the transformer factory pattern used in `transformer_factory.py`**
  - Why use a factory pattern here?
  - How does it handle different tenant types?
  - What would be needed to add a new university/tenant?

#### Data Processing

- **Q: Walk through the `admission_application.py` transformer**

  - What is the purpose of the `transform()` method?
  - How are addresses being processed differently?
  - Explain the education history transformation logic

- **Q: What data validations or quality checks do you see?**
  - How are errors handled?
  - What happens with failed transformations?

#### DataFrame Operations

- **Q: Explain the dataframe operations in `university_cms_dataframe.py`**
  - What is the purpose of filtering by period?
  - How does the class hierarchy work with `BaseDataframe`?

### 3. AWS Glue Job (`data-services-ihe/`)

#### Job Configuration

- **Q: Analyze the Glue job configuration in `admissions-outbound-university-cms.json`**
  - What are the key parameters?
  - How is the job scheduled?
  - What dependencies are being loaded?

#### ETL Process

- **Q: Walk through the `transformation.py` Glue job script**
  - How are the Spark and Glue contexts initialized?
  - Explain the parameter validation at the beginning
  - What is the purpose of the correlation ID?

### 4. Data Publishing & Integration

- **Q: Explain the `publish()` method in the application transformer**
  - Where is data being written?
  - Why are there multiple S3 buckets?
  - What is the SFTP integration doing?
