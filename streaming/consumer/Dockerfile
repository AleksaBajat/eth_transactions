
FROM python:3.11.7
WORKDIR /app
RUN apt-get update && \
    apt-get install default-jdk -y && \
    apt-get clean;

COPY Pipfile Pipfile.lock ./

RUN pip install pipenv && \
    pipenv install --deploy --system


RUN java -version

COPY . .

CMD [ "python3", "consumer.py"]



