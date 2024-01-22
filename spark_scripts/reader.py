import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as sround, avg as savg, max as smax, sum as ssum, unix_timestamp, hour, count, window, coalesce
from logging import info

class Columns:
    DATE = "date"
    HASH = "hash"
    NONCE = "nonce"
    TRANSACTION_INDEX = "transaction_index"
    FROM_ADDRESS = "from_address"
    TO_ADDRESS = "to_address"
    VALUE = "value"
    GAS_UNITS = "gas"
    GAS_PRICE = "gas_price"
    INPUT = "input"
    TOTAL_GAS_USED = "receipt_cumulative_gas_used"
    TRANSACTION_GAS_USED = "receipt_gas_used"
    ADDRESS_CREATED = "receipt_contact_address"
    TRANSACTION_STATUS = "receipt_status"
    BLOCK_TIMESTAMP = "block_timestamp"
    BLOCK_NUMBER = "block_number"
    BLOCK_HASH = "block_hash"
    TOTAL_FEE = "max_fee_per_gas"
    PRIORITY_FEE = "max_priority_fee_per_gas"
    TRANSACTION_TYPE = "transaction_type"
    GAS_PRICE_EF = "receipt_effective_gas_price"
    LAST_MODIFIED = "last_modified"


spark = (SparkSession.builder.master("local")
         .appName("Batch Processing")
         .config("spark.mongodb.output.uri", "mongodb://mongo:27017/eth_transactions")
         .getOrCreate())

namenode_uri = os.environ.get("CORE_CONF_fs_defaultFS");

df = spark.read.parquet(namenode_uri + "/data")


bt_df = (df.select(Columns.GAS_PRICE, Columns.GAS_UNITS, Columns.VALUE, Columns.TRANSACTION_STATUS)
          .withColumn("gas_price_gwei", col(Columns.GAS_PRICE)/1E9)
          .withColumn("gas_price_eth", col(Columns.GAS_PRICE)/1E18)
          .withColumn("gas_price_euro", col(Columns.GAS_PRICE)/1E18 * 1851)
          .withColumn("total_price_euro", col(Columns.GAS_UNITS) * col("gas_price_euro"))
          .withColumn("value_euro", col(Columns.VALUE)/1E18 * 1851)
               ).where(col(Columns.VALUE) != 0.0)
bt_df_sorted = bt_df.orderBy(col("total_price_euro").desc())

bt_df_sorted.write.format("mongo").mode("overwrite").option("database", "eth_transactions").option("collection", "transactions").save()

# SMART CONTRACT INTERACTIONS VS TRANSFERS

contract_interactions = (df.select(Columns.INPUT).where(col(Columns.INPUT) != '0x'))
simple_transfers = (df.select(Columns.INPUT).where(col(Columns.INPUT) == '0x'))

contract_interactions.write.format("mongo").mode("overwrite").option("database", "eth_transactions").option("collection", "contract_interactions").save()
simple_transfers.write.format("mongo").mode("overwrite").option("database", "eth_transactions").option("collection", "simple_transfers").save()


# TOP ACCOUNTS BY TRANSACTION VOLUME

ta_df = (df.select(Columns.FROM_ADDRESS)
         .union(df.select(Columns.TO_ADDRESS))
         .groupBy(Columns.FROM_ADDRESS)
         .count()
         .orderBy(col("count").desc()))

# existing_ta_df = spark.read.format("mongo").option("uri", "mongodb://mongo:27017/eth_transactions").load()
ta_df.write.format("mongo").mode("overwrite").option("database", "eth_transactions").option("collection", "top_accounts").save()
# combined_df = ta_df.join(existing_df, ta_df[Columns.FROM_ADDRESS] == existing_df[Columns.FROM_ADDRESS], "outer")
# updated_df = combined_df.withColumn("count", coalesce(ta_df["count"], 0) + coalesce(existing_df["count"], 0))
# updated_df.write.format("mongo").option("upsert", "true").option("database", "eth_transactions").option("collection", "top_accounts").save()

# MOST ACTIVE HOURS

hourly_transaction_counts = (df.withColumn("hour", hour(Columns.BLOCK_TIMESTAMP))
                                .groupBy("hour")
                                .agg(count("*").alias("num_transactions"))
                                .orderBy("num_transactions", ascending=False))

hourly_transaction_counts.write.format("mongo").mode("overwrite").option("database", "eth_transactions").option("collection", "hourly_transaction_counts").save()

# TRANSACTION SPEED VARIANCE BASED ON GAS PRICE

transaction_speed_df = (df.withColumn("gas_price_gwei", col(Columns.GAS_PRICE)/1E9)
            .withColumn("transaction_duration",
                 unix_timestamp(Columns.LAST_MODIFIED) - unix_timestamp(Columns.BLOCK_TIMESTAMP)) \
                      .groupBy("gas_price_gwei") \
                          .agg(savg("transaction_duration").alias("average_duration")))

transaction_speed_df.write.format("mongo").mode("overwrite").option("database", "eth_transactions").option("collection", "transaction_speed").save()

# BLOCKS REACHING GAS LIMIT

GAS_LIMIT = 30000000  # Maximum size of the block

block_utilization_df = df.groupBy("block_number") \
    .agg(ssum(Columns.TRANSACTION_GAS_USED).alias("total_gas_used")) \
    .withColumn("utilization", (col("total_gas_used") / GAS_LIMIT) * 100)

block_utilization_df.write.format("mongo").mode("overwrite").option("database", "eth_transactions").option("collection", "block_utilization").save()


spark.stop()

