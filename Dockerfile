FROM python:3.10

WORKDIR /app

COPY . /app

RUN pip install --trusted-host pypi.python.org -r requirements.txt

ENV RUN_INTERVAL=120
ENV T1='IngestionTime'
ENV T2='consumerWallClockTime'
ENV VALUE_DESERIALIZER='StringDeserializer'
ENV KEY_DESERIALIZER='StringDeserializer'
ENV ENABLE_SAMPLING='False'
ENV DATE_TIME_FORMAT='epoch'
ENV PRODUCER_CONFIG_FILE='None'
ENV RESULT_DUMP_LOCAL_FILEPATH='None'



