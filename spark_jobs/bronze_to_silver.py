from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

def create_spark_session():
    return (
        SparkSession.builder
        .appName("CryptoMedallion_BronzeToSilver")
        .master("local[*]")
        .getOrCreate()
    )

def read_bronze(spark, bronze_path, date):
    input_path = f"{bronze_path}/coins/date={date}/data.json"
    df = spark.read.option("multiLine", True).json(input_path)
    return df

def clean_and_transform(df):
    df_typed = df.select(
        F.col("id").cast("string").alias("coin_id"),
        F.col("symbol").cast("string").alias("symbol"),
        F.col("name").cast("string").alias("name"),
        F.col("current_price").cast("double").alias("current_price_usd"),
        F.col("market_cap").cast("long").alias("market_cap_usd"),
        F.col("market_cap_rank").cast("long").alias("market_cap_rank"),
        F.col("total_volume").cast("long").alias("total_volume_usd"),
        F.col("price_change_percentage_24h").cast("double").alias("price_change_pct_24h"),
        F.col("circulating_supply").cast("double").alias("circulating_supply"),
        F.col("last_updated").cast("timestamp").alias("last_updated_at"),
    )
    # Handle nulls
    df_clean = df_typed.fillna({
        "price_change_pct_24h": 0.0,
        "circulating_supply": 0.0,
    })

    # Delete nulls
    df_valid = df_clean.filter(
        F.col("current_price_usd").isNotNull() &
        (F.col("current_price_usd") > 0)
    )

    return df_valid

def write_silver(df, silver_path, date):
    output_path = f"{silver_path}/coins"
    
    (
        df.withColumn("ingestion_date", F.lit(date))
        .write
        .mode("overwrite")
        .partitionBy("ingestion_date")
        .parquet(output_path)
    )

def run(bronze_path, silver_path, date):
    spark = create_spark_session()
    try:
        df_bronze = read_bronze(spark, bronze_path, date)
        df_silver = clean_and_transform(df_bronze)
        write_silver(df_silver, silver_path, date)
        print(f"Bronze → Silver complete for date: {date}")
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: bronze_to_silver.py <bronze_path> <silver_path> <date>")
        sys.exit(1)
    
    run(
        bronze_path=sys.argv[1],
        silver_path=sys.argv[2],
        date=sys.argv[3],
    )