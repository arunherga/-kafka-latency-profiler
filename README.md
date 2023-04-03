
# Kafka Latency Profiler

A generic Kafka consumer that measures latency.




## Deployment

To deploy this project run the following command after providing required environment variables in `arguments.env` file. 
Arguments that are not necessary for the usecase can be commented out by using `#` in front. 

```bash
  docker compose build
```

Once the container is built run the following command to run container.

```bash
  docker compose up
``` 


## Environment Variables

To run this project, you will need to add the following environment variables to your .env file

`CONSUMER_CONFIG_FILE`  Path to configuration file that contains properties used to configure a Kafka consumer application to consume messages from a Kafka cluster.

`INPUT_TOPIC`  Kafka topic to consume messages from.

`GROUP_ID`  A string that identifies the consumer group this  consumer     belongs to.

`RUN_INTERVAL`  Duration of time during which the consumer is actively running and consuming messages from a Kafka topic set to 120seconds by defaulf.

`T1`  It is one of time parameter(used to measure latency->t2-t1). value.column name - this is pointer to message value timestamp i.e any column/object in value that points to event time. key.column name - this is pointer to message key timestamp i.e any column/object in key that points to event time. IngestionTime imply time when message is recorded in kafka topic. Set to IngestionTime by default.

`T2` It is one of the time parameter (used to measure latency->t2-t1). ConsumerWallClockTime -this is a pointer to the current time, as seen in the conusumer. IngestionTime imply time when message is recorded in kafka topic.
Choices include - 'consumerWallClockTime' , 'IngestionTime'.

`VALUE_DESERIALIZER` Deserializer class for value if t1 = value.column name. Choices include - 'AvroDeserializer','JSONSchemaDeserializer','StringDeserializer','JSONDeserializer'

`KEY_DESERIALIZER` Deserializer class for key if t1 = key.column name.
Choices include - 'AvroDeserializer','JSONSchemaDeserializer','StringDeserializer','JSONDeserializer'

`DATE_TIME_FORMAT` format of date time and has epoch as default 

`CONSUMER_OUTPUT` Choices include - 'console','localFileDump','dumpToTopic'
'console - Consumer output is printed in console (by default). localFileDump - stores the output of consumer as a csv file and require to be followed by `RESULT_DUMP_LOCAL_FILEPATH`. dumpToTopic - stores consumer output in kafka topic requires to be followed by `PRODUCER_CONFIG_FILE` and  `OUTPUT_TOPIC`

`RESULT_DUMP_LOCAL_FILEPATH` csv file path to store consumer output

`OUTPUT_TOPIC` Kafka topic to dump consumer output

`PRODUCER_CONFIG_FILE` configuration file that contains properties used to configure producer to output topic

`ENABLE_SAMPLING` Enables/Disables sampling. valid arguments are True/False . Default is False


