from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, explode, isnotnull, struct, length, isnull
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, DoubleType, IntegerType
from decouple import config
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def write_eth_transactions(batch_df, batch_id):
    if not batch_df.isEmpty():
        logger.info("WRITING")
        print("WRITING")



        for row in batch_df.rdd.toLocalIterator():
            data = row.data
            timestamp = row.timestamp
            print("DATA {} : TIMESTAMP {}".format(data, timestamp))

        batch_df.select("data").printSchema()

        df_eth_transactions = batch_df.select(
            explode(col("data.data.ethereum.transactions")).alias("transactions"),
            col("timestamp")
        ).select(
            col("transactions.count").alias("count"),
            col("transactions.gasprice").alias("gasprice"),
            col("timestamp")
        )

        df_smart_contract_calls = batch_df.select(
            explode(col("data.data.ethereum.smartContractCalls")).alias("smartContractCalls"),
            col("timestamp")
        ).select(
            col("smartContractCalls.count").alias("count"),
            col("smartContractCalls.amount").alias("amount"),
            col("timestamp")
        )

        (df_eth_transactions.write.format("mongo")
       .option("database", "eth_transactions")
       .option("collection", "real_time_transactions")
       .mode("append").save())


        (df_smart_contract_calls.write.format("mongo")
        .option("database", "eth_transactions")
        .option("collection", "smart_contract_calls")
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
            StructField("smartContractCalls", ArrayType(
                StructType([
                    StructField("count", IntegerType(), False),
                    StructField("amount", DoubleType(), False)
                ]),
            ), False),
            StructField("transactions", ArrayType(
                StructType([
                    StructField("count", IntegerType(), False),
                    StructField("gasPrice", DoubleType(), False)
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
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp")
).filter(col("data").isNotNull())


query_df_parsed = df_parsed.writeStream.foreachBatch(write_eth_transactions).start()

query_df_parsed.awaitTermination()

