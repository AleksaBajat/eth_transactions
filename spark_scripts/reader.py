import os
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


class Columns:
    DATE = "date"
    HASH = "hash"
    NONCE = "nonce"
    TRANSACTION_INDEX = "transaction_index"
    FROM_ADDRESS = "from_address"
    TO_ADDRESS = "to_address"
    VALUE = "value"
    GAS = "gas"
    GAS_PRICE = "gas_price"
    INPUT = "input"
    RECEIPT_CUMULATIVE_GAS_USED = "receipt_cumulative_gas_used"
    RECEIPT_GAS_USED = "receipt_gas_used"
    ADDRESS_CREATED = "receipt_contact_address"
    TRANSACTION_STATUS = "receipt_status"
    BLOCK_TIMESTAMP = "block_timestamp"
    BLOCK_NUMBER = "block_number"
    BLOCK_HASH = "block_hash"
    MAX_FEE_PER_GAS = "max_fee_per_gas"
    PRIORITY_FEE = "max_priority_fee_per_gas"
    TRANSACTION_TYPE = "transaction_type"
    GAS_PRICE_EF = "receipt_effective_gas_price"
    LAST_MODIFIED = "last_modified"


spark = (
    SparkSession.builder.master("local")
    .appName("Batch Processing")
    .config("spark.mongodb.output.uri", "mongodb://mongo:27017/eth_transactions")
    .getOrCreate()
)

namenode_uri = os.environ.get("CORE_CONF_fs_defaultFS")

eth = spark.read.parquet(namenode_uri + "/data")

# GAS PRICE

gas_price_data = (
    eth.select(Columns.HASH, Columns.GAS, Columns.GAS_PRICE)
    .withColumn("total_gas_cost_wei", F.col(Columns.GAS) * F.col(Columns.GAS_PRICE))
    .withColumn("gas_price_gwei", F.col("total_gas_cost_wei") / 1e9)
    .withColumn("gas_price_eth", F.col("total_gas_cost_wei") / 1e18)
    .withColumn("gas_price_euro", (F.col("total_gas_cost_wei") / 1e18) * 1851)
).orderBy(F.desc("gas_price_euro"))

(
    gas_price_data.write.format("mongo")
    .mode("overwrite")
    .option("database", "eth_transactions")
    .option("collection", "gas_pricing")
    .save()
)

# MOST ACTIVE SENDER-RECIEVER PAIRS

sender_receiver_pairs = eth.groupBy(Columns.FROM_ADDRESS, Columns.TO_ADDRESS) \
    .agg(F.count("*").alias("total_transactions")) \
    .orderBy(F.col("total_transactions").desc())

(
    sender_receiver_pairs.write.format("mongo")
    .mode("overwrite")
    .option("database", "eth_transactions")
    .option("collection", "most_active_pairs")
    .save()
)

# GAS USAGE PER ADDRESS

window_spec = Window.partitionBy(Columns.FROM_ADDRESS)
gas_stats = (
    eth.withColumn("avg_gas_used", F.avg(Columns.GAS).over(window_spec))
    .withColumn("max_gas_used", F.max(Columns.GAS).over(window_spec))
    .withColumn("min_gas_used", F.min(Columns.GAS).over(window_spec))
)
gas_stats = gas_stats.select(
    "from_address", "avg_gas_used", "max_gas_used", "min_gas_used"
).distinct()

(
    gas_stats.write.format("mongo")
    .mode("overwrite")
    .option("database", "eth_transactions")
    .option("collection", "gas_usage_by_address")
    .save()
)

# ADDRESS ACTIVITY

window_spec = Window.partitionBy(Columns.FROM_ADDRESS)
active_addresses = eth.withColumn(
    "transaction_count", F.count(Columns.HASH).over(window_spec)
)
active_addresses = (
    active_addresses.select(Columns.FROM_ADDRESS, "transaction_count")
    .distinct()
    .orderBy(F.desc("transaction_count"))
)
(
    active_addresses.write.format("mongo")
    .mode("overwrite")
    .option("database", "eth_transactions")
    .option("collection", "address_activity")
    .save()
)

# ADDRESS TRANSACTION VOLUME

address_transaction_volume = (
    eth.select(Columns.FROM_ADDRESS)
    .union(eth.select(Columns.TO_ADDRESS))
    .groupBy(Columns.FROM_ADDRESS)
    .count()
    .orderBy(F.col("count").desc())
)

(
    address_transaction_volume.write.format("mongo")
    .mode("overwrite")
    .option("database", "eth_transactions")
    .option("collection", "address_transaction_volume")
    .save()
)

# ACTIVE HOURS

hourly_transaction_counts = (
    eth.withColumn("hour", F.hour(Columns.BLOCK_TIMESTAMP))
    .groupBy("hour")
    .agg(F.count("*").alias("num_transactions"))
    .orderBy("num_transactions", ascending=False)
)

(
    hourly_transaction_counts.write.format("mongo")
    .mode("overwrite")
    .option("database", "eth_transactions")
    .option("collection", "hourly_activity")
    .save()
)


# CUMULATIVE GAS USAGE

cumulative_gas_window = Window.partitionBy(F.to_date(Columns.BLOCK_TIMESTAMP), F.hour(Columns.BLOCK_TIMESTAMP))\
                              .orderBy(Columns.BLOCK_TIMESTAMP)\
                              .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate the cumulative gas usage on an hourly basis
hourly_gas_usage = eth.withColumn("hour", F.hour(Columns.BLOCK_TIMESTAMP))\
                      .withColumn("cumulative_gas_used", F.sum(Columns.RECEIPT_CUMULATIVE_GAS_USED).over(cumulative_gas_window))\
                      .groupBy("hour")\
                      .agg(F.max("cumulative_gas_used").alias("total_gas_used"))\
                      .orderBy("hour")

(
    hourly_gas_usage.write.format("mongo")
    .mode("overwrite")
    .option("database", "eth_transactions")
    .option("collection", "hourly_gas_usage")
    .save()
)

# BLOCKS REACHING GAS LIMIT

GAS_LIMIT = 30000000  # Maximum size of the block

block_utilization_df = (
    eth.groupBy("block_number")
    .agg(F.sum(Columns.RECEIPT_GAS_USED).alias("total_gas_used"))
    .withColumn("utilization", (F.col("total_gas_used") / GAS_LIMIT) * 100)
)

(
    block_utilization_df.write.format("mongo")
    .mode("overwrite")
    .option("database", "eth_transactions")
    .option("collection", "block_utilization")
    .save()
)


# GAS USAGE AND FEE ANALYSIS PER TRANSACTION TYPE

stats_by_type = eth.groupBy(Columns.TRANSACTION_TYPE).agg(
    F.avg(Columns.RECEIPT_GAS_USED).alias("avg_gas_used"),
    F.max(Columns.RECEIPT_GAS_USED).alias("max_gas_used"),
    F.min(Columns.RECEIPT_GAS_USED).alias("min_gas_used"),
    F.avg(Columns.GAS_PRICE).alias("avg_gas_price"),
    F.max(Columns.GAS_PRICE).alias("max_gas_price"),
    F.min(Columns.GAS_PRICE).alias("min_gas_price"),
    F.avg(Columns.MAX_FEE_PER_GAS).alias("avg_fee"),
    F.max(Columns.MAX_FEE_PER_GAS).alias("max_fee"),
    F.min(Columns.MAX_FEE_PER_GAS).alias("min_fee")
)

stats_by_type = stats_by_type.withColumn(
    "efficiency_ratio", F.col("avg_gas_used") / F.col("avg_gas_price")
)

stats_by_type = stats_by_type.select(
    Columns.TRANSACTION_TYPE,
    "avg_gas_used",
    "max_gas_used",
    "min_gas_used",
    "avg_gas_price",
    "max_gas_price",
    "min_gas_price",
    "avg_fee",
    "max_fee",
    "min_fee",
    "efficiency_ratio"
)

(
    stats_by_type.write.format("mongo")
    .mode("overwrite")
    .option("database", "eth_transactions")
    .option("collection", "efficiency_analysis")
    .save()
)


## SMART CONTRACT INTERACTIONS VS TRANSFERS

categorized_transactions = eth.withColumn(
    "type",
    F.when(F.col(Columns.INPUT) == '0x', "simple_transfer").otherwise(
        "contract_interaction"
    ),
)

print("COUNT######################################## {}".format(categorized_transactions.count()))

transaction_counts = categorized_transactions.groupBy("type").count()

(
    transaction_counts.write.format("mongo")
    .mode("overwrite")
    .option("database", "eth_transactions")
    .option("collection", "transaction_counts")
    .save()
)


spark.stop()
