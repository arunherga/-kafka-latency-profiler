
# Kafka Latency Profiler

It is a generic Kafka consumer that measures latency.




## Deployment

To deploy this project run the following command after setting required environment variables in `command.env` file . 

```bash
  docker compose up
```


## Environment Variables

To run this project, you will need to add the following environment variables to your .env file

`--consumer_config_file`  Path to configuration file that contains properties used to configure a Kafka consumer application to consume messages from a Kafka cluster.

`--input_topic`  Kafka topic to consume messages from.

`--group_id`  A string that identifies the consumer group this  consumer     belongs to.

`--run_interval`  Duration of time during which the consumer is actively running and consuming messages from a Kafka topic set to 120seconds by defaulf.

`--t1`  It is one of time parameter(used to measure latency->t2-t1). value.column name - this is pointer to message value timestamp i.e any column/object in value that points to event time. key.column name - this is pointer to message key timestamp i.e any column/object in key that points to event time. IngestionTime imply time when message is recorded in kafka topic. Set to IngestionTime by default.

`--t2` It is one of the time parameter (used to measure latency->t2-t1). ConsumerWallClockTime -this is a pointer to the current time, as seen in the conusumer. IngestionTime imply time when message is recorded in kafka topic.
Choices include - 'consumerWallClockTime' , 'IngestionTime'.

`--value_deserializer` Deserializer class for value if t1 = value.column name. Choices include - 'AvroDeserializer','JSONSchemaDeserializer','StringDeserializer','JSONDeserializer'

`--key_deserializer` Deserializer class for key if t1 = key.column name.
Choices include - 'AvroDeserializer','JSONSchemaDeserializer','StringDeserializer','JSONDeserializer'

`--date_time_format` format of date time and has epoch as default 

`--consumer_output` Choices include - 'console','localFileDump','dumpToTopic'
'console - Consumer output is printed in console (by default). localFileDump - stores the output of consumer as a csv file and require to be followed by `--result-dump-local-filepath`. dumpToTopic - stores consumer output in kafka topic requires to be followed by `--producer_config_file` and  `--output_topic`

`--result_dump_local_filepath` csv file path to store consumer output

`--output_topic` Kafka topic to dump consumer output

`--producer_config_file` configuration file that contains properties used to configure producer to output topic


