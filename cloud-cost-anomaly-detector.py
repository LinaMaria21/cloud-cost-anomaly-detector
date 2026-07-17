# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Cloud Cost Anomaly Detector
# MAGIC Uses Spark window functions to compute a rolling 7-day mean/stddev of daily
# MAGIC cost per AWS service, then flags any day where cost exceeds 2 standard
# MAGIC deviations above the rolling mean. Results are checked against the known
# MAGIC injected spikes from `generate_cost_data.py` to validate detection accuracy.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# IMPORTANT: update this to match wherever you uploaded the CSV
# (Catalog -> Add Data -> Upload File will show you the path, typically
# /FileStore/tables/cloud_cost_data.csv)
df = spark.table("workspace.default.cloud_cost_data")

df = df.withColumn("date", F.to_date("date"))
df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rolling 7-day mean & stddev per service

# COMMAND ----------

window_spec = (
    Window.partitionBy("service")
    .orderBy("date")
    .rowsBetween(-7, -1)  # trailing 7 days, excludes current day
)

df_rolling = (
    df
    .withColumn("rolling_mean", F.avg("cost").over(window_spec))
    .withColumn("rolling_std", F.stddev("cost").over(window_spec))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flag anomalies: cost > rolling_mean + 2 * rolling_std

# COMMAND ----------

df_flagged = df_rolling.withColumn(
    "is_anomaly",
    F.when(
        (F.col("rolling_std").isNotNull()) &
        (F.col("cost") > F.col("rolling_mean") + 2 * F.col("rolling_std")),
        1
    ).otherwise(0)
)

anomalies = df_flagged.filter(F.col("is_anomaly") == 1).orderBy("date")
anomalies.select("date", "service", "cost", "rolling_mean", "rolling_std").show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate against known injected spikes

# COMMAND ----------

validation = (
    df_flagged
    .filter(F.col("is_injected_spike") == 1)
    .select("date", "service", "cost", "is_injected_spike", "is_anomaly")
    .orderBy("date")
)

print("Known injected spikes vs. detected:")
validation.show(truncate=False)

detected_count = validation.filter(F.col("is_anomaly") == 1).count()
total_spikes = validation.count()
print(f"Detected {detected_count} of {total_spikes} injected spikes.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chart 1: Cost over time per service, anomalies highlighted

# COMMAND ----------

import matplotlib.pyplot as plt

pdf = df_flagged.toPandas()

fig, ax = plt.subplots(figsize=(14, 6))
for service in pdf["service"].unique():
    subset = pdf[pdf["service"] == service].sort_values("date")
    ax.plot(subset["date"], subset["cost"], label=service, alpha=0.6)

anomaly_points = pdf[pdf["is_anomaly"] == 1]
ax.scatter(anomaly_points["date"], anomaly_points["cost"],
           color="red", s=80, zorder=5, label="Detected anomaly")

ax.set_title("Daily AWS Cost by Service — Anomalies Flagged")
ax.set_xlabel("Date")
ax.set_ylabel("Cost ($)")
ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1))
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chart 2: Detection accuracy summary

# COMMAND ----------

summary = pdf.groupby("service").agg(
    total_days=("cost", "count"),
    anomalies_detected=("is_anomaly", "sum"),
).reset_index()

fig, ax = plt.subplots(figsize=(10, 5))
ax.bar(summary["service"], summary["anomalies_detected"], color="steelblue")
ax.set_title("Anomalies Detected per Service")
ax.set_xlabel("Service")
ax.set_ylabel("Count")
plt.xticks(rotation=30)
plt.tight_layout()
plt.show()
