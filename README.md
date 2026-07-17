Cloud Cost Anomaly Detector

![Uploading chart2.png…]()

A PySpark pipeline that detects unusual spending spikes in AWS cost data using rolling statistical thresholds — built and run on Databricks (Serverless compute).

What it does


Loads daily cost data across 7 AWS services (EC2, S3, Lambda, RDS, DynamoDB, CloudWatch, Data Transfer) over 6 months
Computes a rolling 7-day mean and standard deviation per service using Spark window functions
Flags any day where cost exceeds 2 standard deviations above the rolling mean
Validates results against 6 known, deliberately injected cost spikes
Visualizes cost trends and detected anomalies


Results

Detected 6 out of 6 injected anomalies with zero false negatives.

Show Image

Tech stack


PySpark (window functions, spark.table)
Databricks Serverless compute
Pandas / Matplotlib for visualization


Files


cloud_cost_anomaly_detector.py — main notebook/pipeline
cloud_cost_data.csv — synthetic dataset with injected spikes
generate_cost_data.py — script used to generate the dataset


Why I built this

Practical, hands-on Spark work applied to a real-world FinOps use case: catching unexpected cloud cost spikes before they become a budget problem.
