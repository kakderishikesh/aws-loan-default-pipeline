# AWS Loan Default Pipeline
End-to-end big data pipeline for predicting loan default risk using AWS cloud services.


# Loan Default Prediction using AWS Big Data Pipeline

**For a complete and in-depth walkthrough of this project, including architecture, challenges, and results, please refer to [Final Report.pdf](./Final Report.pdf).**

---

## Project Overview

This project implements a complete big data pipeline to **predict loan default risk** using AWS cloud services. The pipeline covers everything from data ingestion, transformation, and aggregation to machine learning and dashboard visualization. It is built to handle large-scale datasets and provides insights using distributed data processing and cloud-based analytics.

---

## Technologies Used

- **Apache PySpark** on **AWS EC2** – for distributed data cleaning and transformation
- **Amazon S3** – for cloud storage of raw and processed data
- **Amazon SageMaker Autopilot** – for automated ML model training and evaluation
- **Amazon QuickSight** – for interactive dashboards and insights
- **Spark SQL** – for analytical queries on cleaned data

---

## Dataset

- **Source:** [Kaggle – Home Credit Default Risk](https://www.kaggle.com/code/amritachatterjee09/eda-bank-loan-default-risk-analysis/input)
- **Rows:** 307,511  
- **Columns:** 122  
- **Target Column:** `TARGET` (1 = Defaulted, 0 = Repaid Successfully)

---

## Repository Structure

```
AWS-LOAN-DEFAULT-PIPELINE/
├── Aggregated metrics/
│   ├── AggregatedMetric-1.csv
│   ├── AggregatedMetric-2.csv
│   └── ...
├── Spark SQL Outputs/
│   ├── CreditPatternsByHousing.csv
│   ├── LoanTrendsByAgeGroup.csv
│   └── ...
├── application_data.csv
├── cleaned_application_data.csv
├── DataCleaning_Transformation_Aggregation.py
├── SQL_Analysis_PySpark.py
├── Final Report.pdf
├── QuickSight_Dashboard.pdf
└── README.md
```

---

## Pipeline Workflow

### 1. Data Ingestion and Cleaning
- **Stored raw CSV** in an S3 bucket
- **Loaded into PySpark** on an AWS EC2 instance
- Performed:
  - Conversion of negative day values (e.g., `DAYS_BIRTH`)
  - Binning of income, age, and employment
  - Imputation of missing categorical and numeric values

> Output: `cleaned_application_data.csv`

### 2. Data Aggregation (PySpark)
Calculated and exported the following key metrics:
- **Metric 1:** Top 3 longest employment durations
- **Metric 2:** Average income by education type
- **Metric 3:** Applicants by family status
- **Metric 4:** Average credit by housing type
- **Metric 5:** Top 3 occupations by applicant count

> Outputs in `Aggregated metrics/`

### 3. Spark SQL Analysis
Executed SQL queries on the cleaned data:
- Top regions by client rating
- Income trends across education types
- Most common occupations
- Credit amount by housing type
- Loan trends across age groups

> Outputs in `Spark SQL Outputs/`

### 4. Machine Learning (SageMaker Autopilot)
- Trained 10 models using Autopilot
- Best model achieved:
  - **Accuracy:** 80.06%
  - **F1 Score:** 23.30%
  - **AUC-ROC:** 0.674
- Observed strong **class imbalance** (92% non-defaulters), affecting model performance

### 5. Visualization (Amazon QuickSight)
- Built interactive dashboard from S3-stored cleaned dataset
- Highlights:
  - Most defaulters had secondary education
  - Age 50+ had highest repayment rate
  - Married individuals borrowed—and defaulted—the most

> Dashboard exported as `QuickSight_Dashboard.pdf`

---

## Ethical Considerations

- **Data Bias:** High class imbalance affected prediction fairness
- **Privacy:** Dataset had no PII, but discussed risks of using cloud platforms for sensitive financial data

---

## Results & Takeaways

- Built a scalable, end-to-end data pipeline using AWS
- Extracted critical insights from large loan datasets
- Automated machine learning and created visual dashboards
- Highlighted risks of relying solely on accuracy without addressing data imbalance

Certainly! Here's the **Results & Takeaways** section with detailed quantitative insights from your final report:

---

## Results & Takeaways

- Built a scalable, end-to-end big data analytics pipeline using AWS cloud services  
- Processed and analyzed over **307,000** loan applicant records with PySpark  
- Trained **10 models** using Amazon SageMaker Autopilot for loan default prediction  

### Key Quantitative Insights:

- **Default Rate:** Only **8.07%** of the applicants defaulted on their loans  
- **Top Predictors of Default** (based on feature importance from SageMaker):
  - `ORGANIZATION_TYPE`
  - `AMT_GOODS_PRICE`
  - `DAYS_EMPLOYED`

- **Best ML Model Performance:**
  - **Accuracy:** 80.07%
  - **F1 Score:** 23.30%
  - **Precision:** 17.33%
  - **Recall:** 39.10%
  - **AUC-ROC:** 0.674

### Insightful Patterns:

- Most defaulters had **secondary or secondary special education**
- **Married individuals** were the most common borrowers—and also showed the highest default counts  
- Applicants **aged 50+** had the **highest loan repayment success rate**
- Majority of loans were taken by applicants with income in the range of **100K–200K** (normalized units)

### Observations:

- Severe **class imbalance** (92% non-defaulters vs. 8% defaulters) made metrics like F1 score and recall more meaningful than plain accuracy
- While SageMaker Autopilot was efficient, manually tuning models with techniques like SMOTE or XGBoost could potentially yield better results

---

## References

- [Apache Spark Docs](https://spark.apache.org/docs/latest/api/python/index.html)
- [AWS SageMaker Autopilot](https://aws.amazon.com/sagemaker/autopilot/)
- [Amazon QuickSight](https://docs.aws.amazon.com/quicksight/)
- [Hadoop AWS Integration](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html)

---

Rishikesh Kakde (rishikesh.kakde59@gmail.com)