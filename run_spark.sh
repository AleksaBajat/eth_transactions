docker exec -it spark_master bash -c "(cd /spark/bin && chmod -R 775 spark-submit && bash spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 /spark_scripts/reader.py)"
