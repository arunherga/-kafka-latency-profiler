
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
import os

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

    output_dir = '/app/data'
    output_file = os.path.join(output_dir, file_location)

    data.to_csv(output_file, index=False)
    print(f"\nLatency measured written to {file_location} successfully.")

def delivery_report(err, msg):

    if err is not None:
        print("Delivery failed for latency measured {}: {}".format(msg.key(), err))
        return
    print('\n\n Latency measured successfully produced to {} Partition[{}] at offset {} with key {}'.format(msg.topic(), msg.partition(), msg.offset(),msg.key()))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Kafka latency profiler')


    parser.add_argument('--env_file',required=True,help='path to file containing environment variable')


    args = parser.parse_args()

    env_file = args.env_file

    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line:
                key, value = line.split("=")
                os.environ[key] = value
    

    consumer_properties = read_ccloud_config(os.getenv("CONSUMER_CONFIG_FILE"))
    topic=os.getenv("INPUT_TOPIC")
    GI = os.getenv("GROUP_ID")
    consumer_properties['group.id']=os.getenv("GROUP_ID")
    sampling = os.getenv("ENABLE_SAMPLING")
    run_interval=os.getenv("RUN_INTERVAL")
    if run_interval is None:
        run_interval = 120
    else:
        run_interval=int(run_interval)
    t1=os.getenv("T1")
    if t1 is None:
        t1 = 'IngestionTime'
    t2=os.getenv("T2")
    if t2 is None:
        t2 = 'consumerWallClockTime'
    output_type=os.getenv("CONSUMER_OUTPUT")
    local_filepath=os.getenv("RESULT_DUMP_LOCAL_FILEPATH")
    output_topic=os.getenv("OUTPUT_TOPIC")
    value_deserializer = os.getenv("VALUE_DESERIALIZER")
    key_deserializer = os.getenv("KEY_DESERIALIZER")
    producer_properties = read_ccloud_config(os.getenv("PRODUCER_CONFIG_FILE"))
    time_str = os.getenv("DATE_TIME_FORMAT")
    if time_str is None:
        time_str = 'epoch'
    

    # fetching field 

    if t1 != 'IngestionTime':
      
      field = t1.split('.')[1]

      pattern = r'"{}":(\d+)'.format(field)

    print("\n\n\t\t\t\t\t\t\t\t\t\t\tConsumer has started!!\n")


    schema_present = False

    if (key_deserializer == ('AvroDeserializer') or key_deserializer == ('JSONSchemaDeserializer')):
    
        schema_registry =SchemaRegistryClient(read_sr_config(os.getenv("CONSUMER_CONFIG_FILE")))
   
        latest = schema_registry.get_latest_version(f'{topic}-value')
        
        schema = schema_registry.get_schema(latest.schema_id)
   
        schema_str=schema.schema_str
        
        schema_present = True



        if key_deserializer == 'JSONSchemaDeserializer':         

            json_deserializer = JSONDeserializer(schema_str=schema_str)                

        
        
        
        else:
        
            
            avro_deserializer = AvroDeserializer(schema_registry_client=schema_registry,schema_str=schema_str)

   
    

    if (value_deserializer == ('AvroDeserializer') or value_deserializer == ('JSONSchemaDeserializer')):
    
        schema_registry =SchemaRegistryClient(read_sr_config(os.getenv("CONSUMER_CONFIG_FILE")))
   
        latest = schema_registry.get_latest_version(f'{topic}-value')
        
        schema = schema_registry.get_schema(latest.schema_id)
   
        schema_str=schema.schema_str
        
        schema_present = True



        if value_deserializer == 'JSONSchemaDeserializer':         

            json_deserializer = JSONDeserializer(schema_str=schema_str)                

        
        
        
        else:
            
            avro_deserializer = AvroDeserializer(schema_registry_client=schema_registry,schema_str=schema_str)
            

      

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
            
          
            if msg is None:
          
                elapsed_time = time.time()-start_time
                continue
          
          
            if msg.error():
          
          
                if msg.error().code() == KafkaError._PARTITION_EOF:
          
                    print(f'Reached end of partition {msg.topic()}-{msg.partition()}')

                else:
          
                    print(f'Error while consuming from partition {msg.topic()}-{msg.partition()}: {msg.error()}')

                    elapsed_time = time.time()-start_time
          
            else:
          

                if schema_present:
                  


                  if key_deserializer == 'JSONSchemaDeserializer':
            
                    message_value = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                    

            
                  elif key_deserializer == 'AvroDeserializer' :
            
                    message_value = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

          
          
                  elif value_deserializer == 'JSONSchemaDeserializer':
            
                    message_value = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                    

            
                  elif value_deserializer == 'AvroDeserializer' :
            
                    message_value = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                  
                  
                  
                  if message_value is not None:
                      
                      
                      count=count+1
                
                      
                      if t1=="IngestionTime":
                          
                          time1=int(msg.timestamp()[1])
                          
                  
                      elif (t1.split('.')[0]) =='value':
                          #check type 
                       
                            if time_str == "epoch":
                        
                                time1 = message_value[t1.split('.')[1]]
                        
                            else :
                        
                                time_obj = datetime.datetime.strptime(message_value[t1.split('.')[1]], time_str) 
                                
                                time1 = time_obj.timestamp() # returns time1 as a epoch time 
                      
                      elif (t1.split('.')[0]) =='key':
                          #check type 
                       
                            if time_str == "epoch":
                        
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
                
                elif value_deserializer == 'StringDeserializer': #string
                    
                    if msg is not None:
                        
                        count = count+1

                        val = msg.value().decode('utf-8')
                        
                        match = re.search(pattern,val)
                        
                        if t1=="IngestionTime":
                        
                          time1=int(msg.timestamp()[1])
                        
                        
                        elif (t1.split('.')[0]) =='value':
                        
                            time1 = int(match.group(1))
                        
                        if t2 == 'IngestionTime':
                  
                          time2=int(msg.timestamp()[1])
                  
                  
                        elif t2 == 'consumerWallClockTime':
                  
                          time2= time.time()*1000               
                      
                        
                        latency = (time2-time1)
                        
                        latency_arry.append(latency)

                elif key_deserializer == 'StringDeserializer': #string
                    
                    if msg is not None:
                        
                        count = count+1

                        val = msg.key().decode('utf-8')
                        
                        match = re.search(pattern,val)
                        
                        if t1=="IngestionTime":
                        
                          time1=int(msg.timestamp()[1])
                        
                        
                        elif (t1.split('.')[0]) =='key':
                        
                            time1 = int(match.group(1))
                        

                        if t2 == 'IngestionTime':
                  
                          time2=int(msg.timestamp()[1])
                  
                  
                        elif t2 == 'consumerWallClockTime':
                  
                          time2= time.time()*1000               
                      
                        
                        latency = (time2-time1)
                        
                        latency_arry.append(latency)
                
                elif value_deserializer == 'JSONDeserializer':

                    if msg is not None:

                        count = count + 1
                
                        message_value = json.loads(msg.value().decode('utf-8'))

                        if t1=="IngestionTime":
                          
                          time1=int(msg.timestamp()[1])
                          
                  
                        elif (t1.split('.')[0]) =='value':
                            #check type 
                            
                            
                            if time_str == "epoch":
                            
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
                
                elif key_deserializer == 'JSONDeserializer':

                    if msg is not None:

                        count = count + 1
                
                        message_value = json.loads(msg.key().decode('utf-8'))

                        if t1=="IngestionTime":
                          
                          time1=int(msg.timestamp()[1])
                          
                  
                        elif (t1.split('.')[0]) =='key':
                            #check type
                            if time_str == "epoch":
                            
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


        if sampling == 'True':

            length = int(len(latency_arry)*.3)

            random_elements = random.sample(latency_arry,length)

            avg=(sum(random_elements))//(len(random_elements))

            print("\nNumber of message sampled(sampling enabled):\t\t",len(random_elements))

            print("\nAverage Latency in ms:\t\t\t",avg)


        elif sampling == 'False':

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


          schema_registry_client = SchemaRegistryClient(read_sr_config(os.getenv("PRODUCER_CONFIG_FILE")))

          avro_serializer = AvroSerializer(schema_registry_client,schema_string)  

          string_serializer = StringSerializer('utf_8')

          producer=Producer(producer_properties)
            # datetime as key
          producer.produce(topic= output_topic,key=string_serializer(str(f"Topic:{topic},consumer group id:{GI},Date Time:{date_string}")),value=avro_serializer(result, SerializationContext( output_topic, MessageField.VALUE)),on_delivery=delivery_report)     
          
          producer.flush()

        
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
            
        
        



