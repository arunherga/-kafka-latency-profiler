def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf

# def read_kafka_producer_config(config_file):
#     conf = {}
#     with open(config_file) as fh:
#         for line in fh:
#             line = line.strip()
#             if len(line) != 0 and line[0] != "#":
#                 parameter, value = line.strip().split('=', 1)
#                 if parameter == 'key.serializer' or parameter == 'value.serializer':
#                     conf[parameter] = KafkaJsonSchemaSerializer(eval(value))
#                 elif parameter.endswith('.schema'):
#                     conf[parameter] = eval(value)
#                 else:
#                     conf[parameter] = value.strip()
#     return conf

def write_to_csv(file_location, data):
    # Open the file for writing
    with open(file_location, 'w', newline='') as csv_file:
        
        # Create a writer object
        writer = csv.writer(csv_file)

        # Write the data to the CSV file
        for row in data:
            writer.writerow([row])

    print(f"Data written to {file_location} successfully.")

class User(object):
    """
    User record
    """

    def __init__(self, ordertime=None, orderid=None, itemid=None, orderunits=None, address=None):
        self.ordertime = ordertime
        self.orderid = orderid
        self.itemid = itemid
        self.orderunits = orderunits
        self.address = address



def dict_to_user(obj, ctx):
    """
    Converts object literal(dict) to a Order instance.
    """

    if obj is None:
        return None

    return User(ordertime=obj['ordertime'],
             orderid=obj['orderid'],
             itemid=obj['itemid'],
             orderunits=obj['orderunits'],
             address=obj['address'])

import argparse
import time
from confluent_kafka import Consumer, TopicPartition, KafkaError,DeserializingConsumer,Producer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
import random
import json
import csv
import numpy as np


import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Kafka Consumer')

    parser.add_argument('--consumer-config', required=True,help='Absolute path to configuration.properties file that contains settings and properties used to configure a Kafka consumer application to consume messages from a Kafka cluster.')
    parser.add_argument('--bootstrap-servers', required=False, help='A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. list should be in the form host1:port1,host2:port2,...')
    parser.add_argument('--input-topic', required=False, help='Kafka topic to consume messages from.')
    parser.add_argument('--group-id', default="newgroup01", help='A unique string that identifies the consumer group this consumer belongs to. This property is required if the consumer uses either the group management functionality by using subscribe(topic) or the Kafka-based offset management strategy')
    #parser.add_argument('--key-deserializer',help='Deserializer class for key that implements the org.apache.kafka.common.serialization.Deserializer interface.')
    #parser.add_argument('--value-deserializer',help='Deserializer class for value that implements the org.apache.kafka.common.serialization.Deserializer interface.')
    #parser.add_argument('--fetch-min-bytes',help='The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will wait for that much data to accumulate before answering the request. The default setting is 1 byte')
    #parser.add_argument('--heartbeat-interval-ms',help='The expected time between heartbeats to the consumer coordinator when using Kafkas group management facilities. Heartbeats are used to ensure that the consumers session stays active and to facilitate rebalancing when new consumers join or leave the group. The default setting is 3 seconds')
    #parser.add_argument('--enable-auto-commit',type=bool,choices={True,False},help='It determines whether the Kafka consumer should automatically commit its current offset position to the Kafka broker at regular intervals. When enabled to True (which is by default) the consumer will automatically commit the offset based on the "auto.commit.interval.ms" property value. If "enable.auto.commit" is set to "false", the consumer must manually commit the offset position after processing messages.  ')
    #parser.add_argument('--auto-offset-reset',choices={'earliest','latest'},help='It determines what to do when there is no initial offset or when the current offset is out of range.earliest: automatically reset the offset to the earliest offset.latest: automatically reset the offset to the latest offset.')
    parser.add_argument('--enable-sampling',choices={'yes','no'},help='enable/disable sampling by 30 percent')
    parser.add_argument('--run-interval',default=20,type=int,help='duration of time during which the consumer is actively running and consuming messages from a Kafka topic.')
    parser.add_argument('--t1',default='IngestionTime',choices={'IngestionTime','value.EventTimeStamp','key.EventTimeStamp'},help='It is one of time parameter(used to measure latency->t2-t1). value.EventTimeStamp - this is pointer to message value timestamp i.e any column/object in value that points to event time. key.EventTimeStamp - this is pointer to message key timestamp i.e any column/object in key that points to event time. IngestionTime imply time when message is recorded in kafka topic.')
    parser.add_argument('--t2',default='consumerWallClockTime',choices={'consumerWallClockTime','IngestionTime'},help='It is one of the time parameter (used to measure latency->t2-t1). ConsumerWallClockTime -this is a pointer to the current time, as seen in the conusumer. IngestionTime imply time when message is recorded in kafka topic')
    parser.add_argument('--consumer-output',default='console',choices={'console','localFileDump','dumpToTopic'},help='console - Consumer output is printed in console. localFileDump - stores the output of consumer as a csv file(by default) and require to be followed by --result-dump-local-filepath. dumpToTopic - stores consumer output in kafka topic requires to be followed by --result-dump-producer-config')
    parser.add_argument('--result-dump-local-filepath',help='file path to store consumer output ')
    parser.add_argument('--result-dump-producer-config',help=' configuration.properties file that contains settings and properties used to configure a Kafka producer application to dump consumer results' )
    parser.add_argument('--output-topic',help='Kafka topic to dump consumer output')




    args = parser.parse_args()

    props = read_ccloud_config(args.consumer_config)
    topic=args.input_topic
    props['group.id']=args.group_id
    sampling = args.enable_sampling
    run_interval=args.run_interval
    t1=args.t1
    t2=args.t2
    output=args.consumer_output
    local_filepath=args.result_dump_local_filepath
    producer_prop=args.result_dump_producer_config
    output_topic=args.output_topic


    print("consumer configaration used:",props)

    schema_str = """
    {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "ordertime": {
      "type": "integer",
      "description": "The time the order was placed, represented as a Unix timestamp in milliseconds."
    },
    "orderid": {
      "type": "integer",
      "description": "The unique identifier for the order."
    },
    "itemid": {
      "type": "string",
      "description": "The unique identifier for the item being ordered."
    },
    "orderunits": {
      "type": "number",
      "description": "The number of units of the item being ordered, with up to 10 decimal places."
    },
    "address": {
      "type": "object",
      "description": "The shipping address for the order.",
      "properties": {
        "city": {
          "type": "string",
          "description": "The name of the city for the shipping address."
        },
        "state": {
          "type": "string",
          "description": "The name of the state for the shipping address."
        },
        "zipcode": {
          "type": "integer",
          "description": "The 5-digit zip code for the shipping address."
        }
      },
      "required": ["city", "state", "zipcode"]
    }
  },
  "required": ["ordertime", "orderid", "itemid", "orderunits", "address"]
}

    """

    json_deserializer = JSONDeserializer(schema_str,from_dict=dict_to_user)

    consumer=Consumer(props)    
    consumer.subscribe([topic])
    
    topic_partitions = [TopicPartition(topic, p) for p in consumer.list_topics(topic).topics[topic].partitions]
    #print(topic_partitions)
    print("Number of partition in the topic:",len(topic_partitions))

    total_message = 0
    for tp in topic_partitions:
        total_message=total_message+consumer.get_watermark_offsets(tp)[1]
    

    count = 0
    avg = 0
    latency_arry = []
    
    start_time=time.time()
    elapsed_time=0

    try:

        while elapsed_time<run_interval:
            msg = consumer.poll(1.0)
            elapsed_time = time.time()-start_time
            if msg is None:
                print(f"Consumer will close in {int(run_interval-elapsed_time)}seconds")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'Reached end of partition {msg.topic()}-{msg.partition()}')

                else:
                    print(f'Error while consuming from partition {msg.topic()}-{msg.partition()}: {msg.error()}')
            else:
                #mm = msg.value().decode("utf-8")
                #print(f'Consumed message from partition {msg.topic()}-{msg.partition()}, offset {msg.offset()}: {msg.value().decode("utf-8")}')
                #print(mm)
                #print("i am here")
                user = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

                if user is not None:
                    count=count+1
                    
                    print("Order record {}: \n \tordertime: {}\n"
                        "\tItem Number: {}\n"
                        "\tAddress: {}\n"
                        .format(msg.key(), user.ordertime,
                                user.itemid,
                                user.address))
              
                    
                    if t1=="IngestionTime":
                        time1=int(msg.timestamp()[1])
                    elif t1=='value.EventTimeStamp':
                        time1 = user.ordertime
                    
                    if t2 == 'IngestionTime':
                        time2=int(msg.timestamp()[1])
                    elif t2 == 'consumerWallClockTime':
                        time2= time.time()*1000               
                    
                    latency = (time2-time1)
                    latency_arry.append(latency)


    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        
        print("Total Messages in topic:", total_message)
        print("Total Message read by consumer:",count)

        if sampling == 'yes':
            length = int(len(latency_arry)*.3)
            random_elements = random.sample(latency_arry,length)
            avg=sum(random_elements)//len(random_elements)
            print("Number of message sampled(sampling enabled):",len(random_elements))
            print("Average Latency in ms:",avg)
        elif sampling == 'no':
            avg = sum(latency_arry)//count
            print("Number of messages sampled(sampling disabled):",count)
            print("Average latency in ms:",avg)

        print("Qunatiles of the sampled messages:")

        quantiles = np.quantile(latency_arry, [.5, .9, .95, .99, .999])
        print("50 percerntile:",quantiles[0])
        print("90 percentile:",quantiles[1])
        print("95 percentile:",quantiles[2])
        print("99 percentile:",quantiles[3])
        print("99.9 percentile:",quantiles[4])
        

        if output == 'dumpToTopic':
          producer = Producer(read_ccloud_config(producer_prop))
          latency_arry=latency_arry
          for i, element in enumerate(latency_arry):
            key = str(i)  # Convert the index to a string
            value = json.dumps(element)  # Convert the integer to a JSON string
            producer.produce(output_topic, key=key, value=value)
            #producer.produce('topic_6', key=i.to_bytes((count.bit_length() + 7)// 8, byteorder='big'), value=element.to_bytes((count.bit_length() + 7)// 8, byteorder='big'))

          # Wait for any outstanding messages to be delivered and delivery reports to be received
          producer.flush()
          print("Data written successfully to ",output_topic)
        elif output == 'localFileDump':
            
            file_location = local_filepath
            write_to_csv(file_location, latency_arry)
        elif output == 'console':
            print(latency_arry)
            
        
        



