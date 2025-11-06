import dlt
from pyspark.sql.functions import *

# ----------------------------------------
# 🥉 Bronze Layer – Raw Ingest
# ----------------------------------------
@dlt.table(
    comment="Raw journal data loaded from landing zone"
)
def bronze_journal():
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv("/mnt/dlt/journal_lab1/journal_data.csv")
        .withColumn("ingest_time", current_timestamp())
    )

# ----------------------------------------
# ✅ Data Quality Checks on Bronze
# ----------------------------------------
@dlt.table(
    comment="Validated Bronze data with quality checks"
)
@dlt.expect("valid_publication_date", "Publication_Date IS NOT NULL")
@dlt.expect_or_drop("positive_citations", "Citations >= 0")
def bronze_validated():
    return dlt.read("bronze_journal")

# ----------------------------------------
# 🥈 Silver Layer – Data Cleaning & Transformation
# ----------------------------------------
@dlt.table(
    comment="Cleaned journal data with structured fields"
)
def silver_journal():
    df = dlt.read("bronze_validated")

    return (
        df.withColumn("Publication_Date", to_date(col("Publication_Date"), "dd-MM-yyyy"))
          .withColumn("Year", year(col("Publication_Date")))
          .withColumn("Keywords_Array", split(col("Keywords"), ",\\s*"))
    )


# ----------------------------------------
# 🥇 Gold Layer – Aggregated Insights
# ----------------------------------------
@dlt.table(
    comment="Aggregated insights: citations per year"
)
def gold_citations_by_year():
    df = dlt.read("silver_journal")
    return (
        df.groupBy("Year")
          .agg(
              count("*").alias("Total_Papers"),
              sum("Citations").alias("Total_Citations"),
              avg("Citations").alias("Avg_Citations")
          )
          .orderBy("Year")
    )