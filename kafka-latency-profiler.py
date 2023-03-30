
import argparse
import time
from datetime import datetime
from confluent_kafka import Consumer, TopicPartition, KafkaError,Producer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField,StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer,AvroDeserializer
import random
import json
import numpy as np
import pandas as pd
import re

def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    conf.pop('schema.registry.url', None)
    conf.pop('basic.auth.user.info', None)
    conf.pop('basic.auth.credentials.source', None)
    
    return conf

# def config_sorter(conf,use):
#     if (use == 'producer'):
#         req = ['bootstrap.servers','security.protocol','sasl.mechanisms','sasl.username','sasl.password']
#         props = {key: conf[key] for key in req}
#         return props
    
#     #c = conf.copy

#     elif (use == 'sr'):
#         req =['url','basic.auth.user.info']
#         props = {key:conf[key] for key in req}
#         return props
#     elif (use == 'consumer'):
#         req = ['bootstrap.servers','group.id','security.protocol','sasl.mechanisms','sasl.username','sasl.password','auto.offset.reset','enable.auto.commit','connections.max.idle.ms']
#         props = {key:conf[key] for key in req}
#         return props

def read_sr_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    schema_registry_conf = {
        'url':conf['schema.registry.url'],
        'basic.auth.user.info':conf['basic.auth.user.info']}
    
    return schema_registry_conf
   
   

def write_to_csv(file_location, data):          
    data.to_csv(f'{file_location}', index=False)
    print(f"\nLatency measured written to {file_location} successfully.")

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """

    if err is not None:
        print("Delivery failed for latency measured {}: {}".format(msg.key(), err))
        return
    print('\n\n Latency measured successfully produced to {} Partition[{}] at offset {}'.format(msg.topic(), msg.partition(), msg.offset()))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Kafka latency profiler')

    parser.add_argument('--consumer_config_file', required=True,help='Absolute path to configuration file that contains properties used to configure a Kafka consumer application to consume messages from a Kafka cluster.')
    parser.add_argument('--bootstrap_servers', required=False, help='A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. List should be in the form [host1:port1,host2:port2,...]')
    parser.add_argument('--input_topic', required=True, help='Kafka topic to consume messages from.')
    parser.add_argument('--group_id', default="newgroup01", help='A unique string that identifies the consumer group this consumer belongs to.')
    parser.add_argument('--enable_sampling',action="store_true",default=False,help='enable/disable sampling by 30 percent')
    parser.add_argument('--run_interval',default=120,type=int,help='duration of time during which the consumer is actively running and consuming messages from a Kafka topic.')
    parser.add_argument('--t1',default='IngestionTime',help='It is one of time parameter(used to measure latency->t2-t1). value.<column name> - this is pointer to message value timestamp i.e any column/object in value that points to event time. key.<column name> - this is pointer to message key timestamp i.e any column/object in key that points to event time. IngestionTime imply time when message is recorded in kafka topic.')
    parser.add_argument('--t2',default='consumerWallClockTime',choices={'consumerWallClockTime','IngestionTime'},help='It is one of the time parameter (used to measure latency->t2-t1). ConsumerWallClockTime -this is a pointer to the current time, as seen in the conusumer. IngestionTime imply time when message is recorded in kafka topic')
    parser.add_argument('--consumer_output',default='console',choices={'console','localFileDump','dumpToTopic'},help='console - Consumer output is printed in console. localFileDump - stores the output of consumer as a csv file and require to be followed by --result-dump-local-filepath. dumpToTopic - stores consumer output in kafka topic requires to be followed by --producer_config_file --output_topic')
    parser.add_argument('--result_dump_local_filepath',help='csv file path to store consumer output ')
    parser.add_argument('--output_topic',help='Kafka topic to dump consumer output')
    #parser.add_argument('--consumer_schema_json',default='None',help='File path of consumer JsonSchema')
    parser.add_argument('--value_deserializer',required=True,choices={'AvroDeserializer','JSONSchemaDeserializer','StringDeserializer','JSONDeserializer'},help='Deserializer class for value if t1 = value.<column name>.')
    parser.add_argument('--key_deserializer',choices={'AvroDeserializer','JSONSchemaDeserializer','StringDeserializer','JSONDeserializer'},help='Deserializer class for key if t1 = key.<column name>.')
    parser.add_argument('--producer_config_file',help='configuration file file that contains properties used to configure producer to output topic')
    parser.add_argument('--date_time_format',default='epoch',help='format of date time and has epoch as default')



    args = parser.parse_args()
    

    consumer_properties = read_ccloud_config(args.consumer_config_file)
    topic=args.input_topic
    consumer_properties['group.id']=args.group_id
    sampling = args.enable_sampling
    run_interval=args.run_interval
    t1=args.t1
    t2=args.t2
    output_type=args.consumer_output
    local_filepath=args.result_dump_local_filepath
    output_topic=args.output_topic
    #schema_location=args.consumer_schema_json
    value_deserializer = args.value_deserializer
    key_deserializer = args.key_deserializer
    producer_properties = read_ccloud_config(args.producer_config_file)
    time_str = args.date_time_format
    

    # fetching field 

    if t1 != 'IngestionTime':
      
      field = t1.split('.')[1]

      pattern = r'"{}":(\d+)'.format(field)

    print("\n\n\t\t\t\t\t\t\t\t\t\t\tConsumer has started!!\n")
    
    
    


    # if (key_deserializer == ('AvroDeserializer' or 'JSONSchemaDeserializer')):
    
    #     schema_registary =SchemaRegistryClient(read_ccloud_config(args.config_file,'sr'))
   
    #     latest = schema_registary.get_latest_version(f'{topic}-key')
        
    #     schema_str_key = schema_registary.get_schema(latest.schema_id)
   
    #     schema_str_key=schema_str_key.schema_str


        # if key_deserializer == 'JSONSchemaDeserializer':         

        #   #json_deserializer = JSONDeserializer(schema_str,from_dict=dict_to_user)
        #   json_deserializer = JSONDeserializer(schema_str=schema_str_key)
        
        
        # elif key_deserializer == 'AvroDeserializer':
        
        #   schema_registry_client = SchemaRegistryClient(read_ccloud_config(args.config_file,'sr'))
        
        #   avro_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client,schema_str=schema_str_key)
    
    
    # elif key_deserializer == 'StringDeserializer':
    
    #    string_deserializer = StringDeserializer(codec='utf_8')

    schema_present = False

    if (value_deserializer == ('AvroDeserializer') or value_deserializer == ('JSONSchemaDeserializer')):
    
        schema_registary =SchemaRegistryClient(read_sr_config(args.consumer_config_file))
   
        latest = schema_registary.get_latest_version(f'{topic}-value')
        
        schema_str = schema_registary.get_schema(latest.schema_id)
   
        schema_str=schema_str.schema_str
        
        schema_present = True



        if value_deserializer == 'JSONSchemaDeserializer':         

            json_deserializer = JSONDeserializer(schema_str=schema_str)                

        
        
        
        elif value_deserializer == 'AvroDeserializer':
        
            schema_registry_client = SchemaRegistryClient(read_sr_config(args.consumer_config_file))
            
            avro_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client,schema_str=schema_str)
            

      
    
    
    # elif value_deserializer == 'StringDeserializer':
    
    #    string_deserializer = StringDeserializer(codec='utf_8')

    consumer=Consumer(consumer_properties)    
    
    consumer.subscribe([topic])
    
    topic_partitions = [TopicPartition(topic, p) for p in consumer.list_topics(topic).topics[topic].partitions]
    
    print("Number of partition in the topic:",len(topic_partitions))

    total_message = 0
    
    
    # for tp in topic_partitions:
    
    #     total_message=total_message+consumer.get_watermark_offsets(tp)[1]
    
    # print("\nTotal Messages in topic\t:\t", total_message)
    
    count = 0
    
    avg = 0
    
    latency_arry = []
    
    start_time=time.time()
    
    elapsed_time=0    
    

    

    try:

        while elapsed_time<run_interval:
          
            msg = consumer.poll(1.0)          
            
            #print(elapsed_time)
          
            if msg is None:
          
                # print(f"Consumer will close in {int(run_interval-elapsed_time)}seconds")
                #print("waiting")
                elapsed_time = time.time()-start_time
                continue
          
          
            if msg.error():
          
          
                if msg.error().code() == KafkaError._PARTITION_EOF:
          
                    print(f'Reached end of partition {msg.topic()}-{msg.partition()}')

                else:
          
                    print(f'Error while consuming from partition {msg.topic()}-{msg.partition()}: {msg.error()}')

                    elapsed_time = time.time()-start_time
          
            else:
          

                if (schema_present == True):

          
          
                  if value_deserializer == 'JSONSchemaDeserializer':
            
                    message_value = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                    

            
                  elif value_deserializer == 'AvroDeserializer':
            
                    message_value = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                  
                  
                  # if deserializer == 'StringDeserializer':
                  #     user = string_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))


                  
            
                  #if ((user is not None) and (value_deserializer == ('JSONSchemaDeserializer' or 'AvroDeserializer'))) :
                  if message_value is not None:
                      
                      
                      count=count+1
                
                      
                      if t1=="IngestionTime":
                          
                          time1=int(msg.timestamp()[1])
                          
                  
                      elif (t1.split('.')[0]) =='value':
                          #check type 
                       
                        if time_str == 'epoch':
                       
                            time1 = message_value[t1.split('.')[1]]
                       
                        else :
                       
                            time_obj = datetime.datetime.strptime(message_value[t1.split('.')[1]], time_str) 
                            
                            time1 = time_obj.timestamp() # returns time1 as a epoch time 

                            
                  
                  
                      # elif (t1.split('.')[0]) == 'key':
                  
                      #     #i = t1.split('.')[1]
                      #     #time1 = getattr(user,i)
                      #     user1 = json_deserializer(msg.key(), SerializationContext(msg.topic(), MessageField.VALUE))
                      #     time1 = user1[t1.split('.')[1]]               

                      
                      if t2 == 'IngestionTime':
                  
                          time2=int(msg.timestamp()[1])
                  
                  
                      elif t2 == 'consumerWallClockTime':
                  
                          time2= time.time()*1000               
                      
                      latency = (time2-time1)

                      latency_arry.append(latency)
                
                if value_deserializer == 'StringDeserializer': #string
                    
                    if msg is not None:
                        
                        count = count+1

                        val = msg.value().decode('utf-8')
                        
                        match = re.search(pattern,val)
                        
                        if t1=="IngestionTime":
                        
                          time1=int(msg.timestamp()[1])
                        
                        
                        elif (t1.split('.')[0]) =='value':
                        
                            time1 = int(match.group(1))

                        # elif (t1.split('.')[0]) =='value':
                        #   #check type 
                        #     if time_str == 'epoch':
                        #         time1 = int(match.group(1))
                        #     else :
                        #         time_obj = datetime.datetime.strptime(int(match.group(1)), time_str)
                        #         time1 = time_obj.timestamp()
                        

                        if t2 == 'IngestionTime':
                  
                          time2=int(msg.timestamp()[1])
                  
                  
                        elif t2 == 'consumerWallClockTime':
                  
                          time2= time.time()*1000               
                      
                        
                        latency = (time2-time1)
                        
                        latency_arry.append(latency)
                
                if value_deserializer == 'JSONDeserializer':

                    if msg is not None:
                
                        message_value = json.loads(msg.value().decode('utf-8'))

                        if t1=="IngestionTime":
                          
                          time1=int(msg.timestamp()[1])
                          
                  
                        elif (t1.split('.')[0]) =='value':
                            #check type 
                            
                            
                            if time_str == 'epoch':
                            
                                time1 = message_value[t1.split('.')[1]]
                            
                            
                            else :
                            
                                time_obj = datetime.datetime.strptime(message_value[t1.split('.')[1]], time_str) 
                            
                                time1 = time_obj.timestamp() # returns time1 as a epoch time 
                        

                        if t2 == 'IngestionTime':
                  
                          time2=int(msg.timestamp()[1])
                  
                  
                        elif t2 == 'consumerWallClockTime':
                    
                            time2= time.time()*1000               
                        
                        latency = (time2-time1)

                        latency_arry.append(latency)
                        

        
            elapsed_time = time.time()-start_time



    except KeyboardInterrupt:
        
        pass
    
    finally:
        
        consumer.close()

        n = datetime.now()
        
        date_string = n.strftime("%Y-%m-%d %H:%M:%S.%f")
        

        
        print("\nTotal Message read by consumer\t:\t",count)

        print("\nCurrent Time\t:\t",date_string)

        # with open('numbers.csv', 'w', newline='') as file:
        #   writer = csv.writer(file)

        #   # Write the array to the CSV file
        #   writer.writerow(latency_arry)

        if sampling == True:

            length = int(len(latency_arry)*.3)

            random_elements = random.sample(latency_arry,length)

            avg=(sum(random_elements))//(len(random_elements))

            print("\nNumber of message sampled(sampling enabled):\t\t",len(random_elements))

            print("\nAverage Latency in ms:\t\t\t",avg)


        elif sampling == False:

            avg = (sum(latency_arry))//count

            print("\nNumber of messages sampled(sampling disabled):\t\t",count)

            print("\nAverage latency in ms:\t\t",avg)


        print("\n\nQuantiles of the latencies measured in ms:")


        quantiles = np.quantile(latency_arry, [.5, .9, .95, .99, .999])

        print("\n\t50th percerntile:\t",quantiles[0])

        print("\n\t90th percentile:\t",quantiles[1])

        print("\n\t95th percentile:\t",quantiles[2])

        print("\n\t99th percentile:\t",quantiles[3])

        print("\n\t99.9th percentile:",quantiles[4])
        

        if output_type == 'dumpToTopic':

          schema_string ="""
            {
              "namespace": "example.avro",
              "type": "record",
              "name": "result",
              "fields": [
                  {"name": "average", "type": "int"},
                  {"name": "percentile50", "type": "int"},
                  {"name": "percentile90", "type": "int"},
                  {"name": "percentile95", "type": "int"},
                  {"name": "percentile99", "type": "int"},
                  {"name": "percentile999", "type": "int"},
                  {"name": "Date_Time", "type": "string"}
              ]
            }
            """

          result = {
                    "average": int(avg),
                    "percentile50": int(quantiles[0]),
                    "percentile90": int(quantiles[1]),
                    "percentile95": int(quantiles[2]),
                    "percentile99": int(quantiles[3]),
                    "percentile999": int(quantiles[4]),
                    "Date_Time": str(date_string)
                  }


          schema_registry_client = SchemaRegistryClient(read_sr_config(args.producer_config_file))

          avro_serializer = AvroSerializer(schema_registry_client,schema_string)  

          string_serializer = StringSerializer('utf_8')

          producer=Producer(producer_properties)
            # datetime as key
          producer.produce(topic= output_topic,key=string_serializer(str(f"Topic:{topic},consumer group id:{args.group_id},Date Time:{date_string}")),value=avro_serializer(result, SerializationContext( output_topic, MessageField.VALUE)),on_delivery=delivery_report)     
          
          producer.flush()
          
          #print("\n\nData written successfully to :\t",output_topic)

        
        elif (output_type == 'localFileDump'):
                      
          df = pd.DataFrame({'average': [avg],
                   'quantile_50': quantiles[0],
                   'quantile_90': quantiles[1],
                   'quantile_90': quantiles[2],
                   'quantile_99': quantiles[3],
                   'quantile_99.9':quantiles[4],
                   'date_time':date_string})
            
          write_to_csv(local_filepath, df)

                       
        
        # elif output_type == 'console':
        #     print(latency_arry)

        print("\n\n\t\t\t\t\t\t\t\t\t\t\tconsumer closing\n\n")
            
        
        



