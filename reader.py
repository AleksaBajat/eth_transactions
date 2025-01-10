import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as sround, avg as savg, max as smax, sum as ssum, unix_timestamp, hour, count, window

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
         #.config("spark.mongodb.output.uri", "mongodb://mongo:27017/data")
         .getOrCreate())

namenode_uri = os.environ.get("CORE_CONF_fs_defaultFS");

df = spark.read.parquet(namenode_uri + "/data")

reference_df = (df.select(Columns.GAS_UNITS, Columns.GAS_PRICE, Columns.VALUE)
          .withColumn("gas_price_gwei", col(Columns.GAS_PRICE)/1E9)
          .withColumn("gas_price_eth", col(Columns.GAS_PRICE)/1E18)
          .withColumn("gas_price_euro", col(Columns.GAS_PRICE)/1E18 * 1851)
          .withColumn("gas_price_total_units_euro", sround(col("gas_price_euro")*col(Columns.GAS_UNITS), 2))
          .withColumn("eth_transfered", col(Columns.VALUE)/1E18)
          .withColumn("euro_transfered", col(Columns.VALUE)/1E18 * 1851))

# AVERAGE PRICE OF GAS

pog_df = (df.select(Columns.GAS_PRICE)
          .withColumn("gas_price_gwei", col(Columns.GAS_PRICE)/1E9)
          .withColumn("gas_price_eth", col(Columns.GAS_PRICE)/1E18)
          .withColumn("gas_price_euro", col(Columns.GAS_PRICE)/1E18 * 1851)
               )


average_price = pog_df.agg(savg('gas_price_gwei')).collect()[0][0]

print("Average price of gas in gwei : {}".format(average_price))


# AVERAGE TRANSACTION PRICE

tp_df = (df.select(Columns.GAS_PRICE, Columns.GAS_UNITS)
          .withColumn("gas_price_gwei", col(Columns.GAS_PRICE)/1E9)
          .withColumn("gas_price_eth", col(Columns.GAS_PRICE)/1E18)
          .withColumn("gas_price_euro", col(Columns.GAS_PRICE)/1E18 * 1851)
               )

average_tp = tp_df.agg(savg(col(Columns.GAS_UNITS) * col("gas_price_euro"))).collect()[0][0]
print("Average price of transaction in euro: {}".format(average_tp))

# BIGGEST TRANSACTIONS

bt_df = (df.select(Columns.GAS_PRICE, Columns.GAS_UNITS, Columns.VALUE)
          .withColumn("gas_price_gwei", col(Columns.GAS_PRICE)/1E9)
          .withColumn("gas_price_eth", col(Columns.GAS_PRICE)/1E18)
          .withColumn("gas_price_euro", col(Columns.GAS_PRICE)/1E18 * 1851)
          .withColumn("total_price_euro", col(Columns.GAS_UNITS) * col("gas_price_euro"))
          .withColumn("value_euro", col(Columns.VALUE)/1E18 * 1851)
               ).where(col(Columns.VALUE) != 0.0)
bt_df_sorted = bt_df.orderBy(col("total_price_euro").desc())

bt_df_sorted.show(10, truncate=False)

# TOP ACCOUNTS BY TRANSACTION VOLUME

ta_df = (df.select(Columns.FROM_ADDRESS)
         .union(df.select(Columns.TO_ADDRESS))
         .groupBy(Columns.FROM_ADDRESS)
         .count()
         .orderBy(col("count").desc()))

ta_df.show(10, truncate=False)


# FAILED TRANSACTIONS PERCENTAGE

# ftp_df = (df.select(Columns.TRANSACTION_STATUS).where(Columns.TRANSACTION_STATUS == 0).count())
ftp_df = (df.select(Columns.TRANSACTION_STATUS).groupBy(Columns().TRANSACTION_STATUS).count())

results = ftp_df.collect()

failed_count = results[0][1]
success_count = results[1][1]

ratio = failed_count / (success_count + failed_count)

print("Failed transactions : {}%".format(round(ratio * 100, 4)))


# AVERAGE GAS USED BY TRANSACTION

agt_df = (df.select(Columns.GAS_UNITS).agg(savg(Columns().GAS_UNITS))).collect()[0][0]
print("Average gas used up by transaction: {}".format(round(agt_df, 2)))


# SMART CONTRACT INTERACTIONS VS TRANSFERS

print("#########")
contract_interactions_count = (df.select(Columns.INPUT).where(col(Columns.INPUT) != '0x').count())
simple_transfers_count = (df.select(Columns.INPUT).where(col(Columns.INPUT) == '0x').count())

percentage = round(contract_interactions_count/(simple_transfers_count + contract_interactions_count) * 100, 4)

print("Smart contract interaction percentage: {}%".format(round(ratio * 100, 4)))
print("Simple transfer percentage: {}%".format(round((100 - ratio) * 100, 4)))


# BLOCKS REACHING GAS LIMIT

GAS_LIMIT = 30000000  # Maximum size of the block

block_utilization_df = df.groupBy("block_number") \
    .agg(ssum(Columns.TRANSACTION_GAS_USED).alias("total_gas_used")) \
    .withColumn("utilization", (col("total_gas_used") / GAS_LIMIT) * 100)

# spark_stream = SparkSession.builder.appName("Stream processing").getOrCreate()
#
#
# # TRANSACTION SPEED VARIANCE BASED ON GAS PRICE
#
# transaction_speed_df = (df.withColumn("gas_price_gwei", col(Columns.GAS_PRICE)/1E9)
#             .withColumn("transaction_duration",
#                  unix_timestamp(Columns.LAST_MODIFIED) - unix_timestamp(Columns.BLOCK_TIMESTAMP)) \
#                       .groupBy("gas_price_gwei") \
#                           .agg(savg("transaction_duration").alias("average_duration")))
#
# transaction_speed_df.show()
#
# # MOST ACTIVE HOURS
#
# hourly_transaction_counts = (df.withColumn("hour", hour(Columns.BLOCK_TIMESTAMP))
#                                 .groupBy("hour")
#                                 .agg(count("*").alias("num_transactions"))
#                                 .orderBy("num_transactions", ascending=False))
#
# hourly_transaction_counts.show()
#
# spark.stop()
#
