import pytz
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from decouple import config
import requests, json, traceback
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = config("KAFKA_BROKER", cast=str)
logger.info("KAFKA BROKER: {}".format(KAFKA_BROKER))
BITQUERY_API_KEY = config("BITQUERY_API_KEY", cast=str)
logger.info("BITQUERY_API_KEY: {}".format(BITQUERY_API_KEY))

class Topics:
    REAL_TIME_ETH = "real_time_eth"



def run_query(query:str) -> dict:
    headers = {'X-API-KEY' : BITQUERY_API_KEY }
    request = requests.post('https://graphql.bitquery.io/', json={'query':query}, headers=headers)

    if request.status_code == 200:
        return request.json()
    else:
        raise Exception('Query failed and return code is {}.        {}'.format(request.status_code, query))



def create_query() -> str:
    end_time = datetime.now(pytz.utc) - timedelta(minutes=5)
    start_time = end_time - timedelta(minutes=10)

    start_time_iso = start_time.strftime('%Y-%m-%dT%H:%M:%S')
    end_time_iso = end_time.strftime('%Y-%m-%dT%H:%M:%S')

    query = f'''{{
  ethereum {{
    transactions(
        time: {{since: "{start_time_iso}", till: "{end_time_iso}"}}
        options: {{ desc: "block.timestamp.iso8601", limit: 100 }})
        {{
          block {{
            timestamp {{
              iso8601
            }}
          }}
          gas
          gasPrice
          gasValue
          hash
          nonce
          sender {{
            address
          }}
          to {{
            address
          }}
          txType
        }}
    }}
  }}
'''

    return query


def get_kafka_producer(broker_url):
    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            if producer.bootstrap_connected():
                logger.info("Kafka broker is available.")
                return producer
            else:
                logger.warning("Failed to connect to Kafka broker.")
                time.sleep(15)
                logger.warning("Retrying to connect to Kafka broker...")
                continue;
        except KafkaError as e:
            logger.warning("Failed to connect to Kafka broker.")
            time.sleep(15)
            logger.warning("Retrying to connect to Kafka broker...")


producer = get_kafka_producer(KAFKA_BROKER)

while True:
    try:
        query = create_query()
        logger.info("Query : {}".format(query))
        query_result = run_query(query)
        if not "errors" in query_result:
            logger.info("Query Result: {}".format(query_result))
            producer.send(Topics.REAL_TIME_ETH, value=query_result)
            time.sleep(300)
        else:
            logger.error("Query Result: {}".format(query_result))
            time.sleep(15)
    except Exception as e:
        traceback.print_exception(e)
        time.sleep(15)

