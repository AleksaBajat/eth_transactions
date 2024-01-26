from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, DoubleType, IntegerType, LongType
from decouple import config
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def write_eth_transactions(batch_df, batch_id):
    if not batch_df.isEmpty():
        logger.info("WRITING")
        print("WRITING")

        ## Count of each transaction type in batch
        exploded_df = batch_df.select(
            F.explode(F.col("data.data.ethereum.transactions")).alias("transaction")
        )

        window_spec = Window.partitionBy("transaction.txType").orderBy("transaction.hash")

        tx_type_running_count = exploded_df.withColumn(
            "running_count",
            F.row_number().over(window_spec)
        ).withColumn(
            "txType",
            F.when(F.col("transaction.txType") != "", F.col("transaction.txType")).otherwise(2)
        ).select(
            F.col("txType"),
            F.col("running_count")
        )

        (tx_type_running_count.write.format("mongo")
       .option("database", "eth_transactions")
       .option("collection", "rt_transaction_type_count")
       .mode("append").save())

        # Cumulative average gas price and gas used
#        for row in exploded_df.toLocalIterator():
#            logging.info(row)
#
#        avg_gas_metrics = exploded_df.agg(
#            F.avg(F.col("transaction.gas").cast("float")).alias("average_gas"),
#            F.avg(F.col("transaction.gasPrice").cast("float")).alias("average_gas_price")
#        ).select("average_gas", "average_gas_price")
#
#        for row in avg_gas_metrics.toLocalIterator():
#            logging.info(row)
#
#        (avg_gas_metrics.write.format("mongo")
#       .option("database", "eth_transactions")
#       .option("collection", "rt_average_gas")
#       .mode("append").save())

        # Transactions by gas value

        window_spec = Window.partitionBy("transaction.hash").orderBy(F.desc("transaction.gasValue"))

        top_gas_value_transactions_ranked = exploded_df.withColumn(
            "gas_value_rank",
            F.rank().over(window_spec)
            ).select("transaction.hash","transaction.gasValue", "gas_value_rank")

        (top_gas_value_transactions_ranked.write.format("mongo")
       .option("database", "eth_transactions")
       .option("collection", "rt_transactions_value")
       .mode("append").save())

        # Cumulative count of transactions by sender address

        window_spec = Window.partitionBy("transaction.sender.address").orderBy("transaction.hash").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

        transactions_by_sender_cumulative = exploded_df.withColumn(
            "cumulative_count",
            F.count("transaction.hash").over(window_spec)
        ).select("transaction.sender.address", "cumulative_count")

        (transactions_by_sender_cumulative.write.format("mongo")
       .option("database", "eth_transactions")
       .option("collection", "rt_transactions_by_sender")
       .mode("append").save())

        # Nonce value summary - number of transactions prior

        window_spec = Window.orderBy("transaction.hash").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

        nonce_summary_running = exploded_df.withColumn(
            "running_min_nonce",
            F.min("transaction.nonce").over(window_spec)
        ).withColumn(
            "running_max_nonce",
            F.max("transaction.nonce").over(window_spec)
        ).withColumn(
            "running_avg_nonce",
            F.avg("transaction.nonce").over(window_spec)
        ).select("transaction.hash","transaction.sender.address", "running_min_nonce", "running_max_nonce", "running_avg_nonce")

        (nonce_summary_running.write.format("mongo")
       .option("database", "eth_transactions")
       .option("collection", "rt_nonce_summary")
       .mode("append").save())


        logger.info("SAVED")
        print("SAVED")
    else:
        logger.info("SKIPPING WRITE")


spark = SparkSession.builder \
    .appName("KafkaEthConsumer") \
    .config("spark.mongodb.output.uri", "mongodb://mongo:27017/eth_transactions") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
    .getOrCreate()


kafka_broker = config("KAFKA_BROKER", cast=str)
logger.info("KAFKA BROKER: {}".format(kafka_broker))

topic = "real_time_eth"

schema = StructType([
    StructField("data", StructType([
        StructField("ethereum", StructType([
            StructField("transactions", ArrayType(
                    StructType([
                    StructField("block", StructType([
                        StructField("timestamp", StructType([
                            StructField("iso8601", StringType(), True)
                        ]), True)
                    ]), True),
                    StructField("gas", DoubleType(), True),
                    StructField("gasPrice", DoubleType(), True),
                    StructField("gasValue", DoubleType(), True),
                    StructField("hash", StringType(), True),
                    StructField("nonce", LongType(), True),
                    StructField("sender", StructType([
                        StructField("address", StringType(), True)
                    ]), True),
                    StructField("to", StructType([
                        StructField("address", StringType(), True)
                    ]), True),
                    StructField("txType", StringType(), True)
                ])
            ), False)
        ]), False)
    ]), False)
])

#df_raw = spark.readStream \
#    .format("kafka") \
#    .option("kafka.bootstrap.servers", kafka_broker) \
#    .option("subscribe", topic) \
#    .load()
#


logger.info("READING STREAM")
while True:
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_broker) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
        df.printSchema()
        break
    except Exception as e:
        time.sleep(15)


df_parsed = df.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data"),
    F.col("timestamp")
).filter(F.col("data").isNotNull())


query_df_parsed = df_parsed.writeStream.foreachBatch(write_eth_transactions).start()

query_df_parsed.awaitTermination()

