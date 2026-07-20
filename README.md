Cloud Cost Anomaly Detector


<img width="1393" height="1006" alt="chart3" src="https://github.com/user-attachments/assets/a31bade1-2ea1-4029-ac43-b7f89e9df5bd" />
<img width="1362" height="561" alt="chart1" src="https://github.com/user-attachments/assets/7d254516-5b04-481a-9773-6a22a405c998" />
<img width="1353" height="638" alt="chart2" src="https://github.com/user-attachments/assets/1d45081a-cb5c-44d0-90f8-ad7211f927d7" />


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
