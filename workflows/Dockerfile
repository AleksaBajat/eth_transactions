FROM python:3.11.7
ENV AIRFLOW_HOME=/app/airflow
WORKDIR /app
COPY Pipfile Pipfile.lock ./

RUN pip install pipenv && \
    pipenv install --deploy --system

COPY ./dags $AIRFLOW_HOME/dags

RUN airflow db init

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT [ "bash", "entrypoint.sh" ]



