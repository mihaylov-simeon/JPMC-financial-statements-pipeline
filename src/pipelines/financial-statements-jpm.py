from pyspark.sql.functions import (
    col, lit, to_date, trim, lag, when, round
)
from pyspark.sql.types import DecimalType
from pyspark.sql.session import SparkSession
from functools import reduce
from pyspark.sql.window import Window

# Data paths
BRONZE_PATH = "data/bronze/JPM_balance_sheet.csv"
SILVER_PATH = "data/silver/financial_statements_jpm_silver"
GOLD_PATH = "data/gold/financial_statements_jpm_gold"


def main():
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("FinancialStatementsJPM")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read the raw data from the "bronze" layer
    raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(BRONZE_PATH)
    )

    # Take all date columns from the dataset
    date_columns = [c for c in raw.columns if c != "CATEGORY"]

    # List of per_data_dataframes
    per_date_dfs = []

    for c in date_columns:

        df_for_one_date = (
            raw.select(
                col("CATEGORY"),
                lit(c).alias("TRX_DT"),
                col(c).cast(DecimalType(20, 2)).alias("TRX_AMT")
            )
        )
        per_date_dfs.append(df_for_one_date)

    raw.write.mode("overwrite").parquet("data/bronze/financial_statements_jpm_bronze")

    silver = reduce(lambda df1, df2: df1.unionByName(df2), per_date_dfs)

    silver = silver.withColumn("TRX_DT", to_date(col("TRX_DT"), "yyyy-MM-dd"))
    silver = silver.withColumn("CATEGORY", trim(col("CATEGORY")))

    silver.write.mode("overwrite").parquet(SILVER_PATH)

    w = Window.partitionBy("CATEGORY").orderBy("TRX_DT")

    gold = (
        silver
        .withColumn(
            "PREV_TRX_AMT", lag("TRX_AMT").over(w))
        .withColumn(
            "YOY_CHG", col("TRX_AMT") - col("PREV_TRX_AMT"))
        .withColumn(
            "YOY_PCT_CHG", when(
                (
                    col("PREV_TRX_AMT").isNull()) | (col("PREV_TRX_AMT") == 0), 
                    lit(None)
                ).otherwise(
                    round(
                        ((col("TRX_AMT") - col("PREV_TRX_AMT")) / col("PREV_TRX_AMT")) * 100, 2)
                    )
            )
        )

    gold = gold.select(
        'CATEGORY', 
        'TRX_DT', 
        'TRX_AMT', 
        'YOY_CHG', 
        'YOY_PCT_CHG')
    
    gold.write.mode("overwrite").parquet(GOLD_PATH)

    gold.show(10, False)

if __name__ == "__main__":
    main()
