#!/bin/bash

airflow users create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
airflow scheduler -D && airflow webserver -p 9080 -D
