from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys

def create_spark_session():
    return (
        SparkSession.builder
        .appName("CryptoMedallion_SilverToGold")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

def read_silver(spark, silver_path, date):
    input_path = f"{silver_path}/coins/ingestion_date={date}"
    df = spark.read.parquet(input_path)
    return df

def build_top_coins(df):
    window = Window.orderBy(F.col("market_cap_usd").desc())
    
    return (
        df.withColumn("rank", F.rank().over(window))
        .filter(F.col("rank") <= 10)
        .select(
            "rank",
            "coin_id",
            "symbol",
            "name",
            "current_price_usd",
            "market_cap_usd",
            "total_volume_usd",
            "price_change_pct_24h",
        )
        .orderBy("rank")
    )

def build_market_summary(df):
    total_market_cap = df.agg(F.sum("market_cap_usd")).collect()[0][0]
    btc_market_cap = df.filter(F.col("coin_id") == "bitcoin") \
        .agg(F.sum("market_cap_usd")).collect()[0][0] or 0
    
    btc_dominance = round((btc_market_cap / total_market_cap * 100), 2) if total_market_cap else 0

    return df.agg(
        F.count("coin_id").alias("total_coins_tracked"),
        F.sum("market_cap_usd").alias("total_market_cap_usd"),
        F.sum("total_volume_usd").alias("total_volume_usd"),
        F.avg("price_change_pct_24h").alias("avg_market_change_pct_24h"),
        F.sum(F.when(F.col("price_change_pct_24h") > 0, 1).otherwise(0)).alias("coins_in_green"),
        F.sum(F.when(F.col("price_change_pct_24h") <= 0, 1).otherwise(0)).alias("coins_in_red"),
    ).withColumn("btc_dominance_pct", F.lit(btc_dominance))

def write_gold(df, gold_path, table_name, date):
    output_path = f"{gold_path}/{table_name}"
    
    (
        df.withColumn("ingestion_date", F.lit(date))
        .write
        .mode("overwrite")
        .partitionBy("ingestion_date")
        .parquet(output_path)
    )

def run(silver_path, gold_path, date):
    spark = create_spark_session()
    try:
        df_silver = read_silver(spark, silver_path, date)
        
        df_top = build_top_coins(df_silver)
        write_gold(df_top, gold_path, "top_coins_by_marketcap", date)
        
        df_summary = build_market_summary(df_silver)
        write_gold(df_summary, gold_path, "market_summary", date)
        
        print(f"Silver → Gold complete for date: {date}")
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: silver_to_gold.py <silver_path> <gold_path> <date>")
        sys.exit(1)
    
    run(
        silver_path=sys.argv[1],
        gold_path=sys.argv[2],
        date=sys.argv[3],
    )