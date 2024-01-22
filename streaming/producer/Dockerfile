
FROM python:3.11.7
WORKDIR /app
COPY Pipfile Pipfile.lock ./

RUN pip install pipenv && \
    pipenv install --deploy --system

COPY . .

CMD [ "python3", "producer.py"]



