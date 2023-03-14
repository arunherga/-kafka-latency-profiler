import argparse
from confluent_kafka import Consumer, KafkaError
import time
import re
import config_2
import producer_latency
import numpy as np

# def create_consumer(args):
#     conf = {
#         'bootstrap.servers': args.bootstrap_servers,
#         'group.id': args.group_id,
#         "security.protocol": "SASL_SSL",
#         "sasl.mechanisms": "PLAIN",
#         "sasl.username": "4WSOEINJVRBJDMBB",
#         "sasl.password": "J58R0DMyN8n1OMIiAj2wOpQ2WrePSO05YXQ63dwmzN7WPIG2UOphSf+rL4WX1WeW",
#         'auto.offset.reset': args.auto_offset_reset,
#         'enable.auto.commit': args.enable_auto_commit,
#         'connections.max.idle.ms': 1200
#     }
#     consumer = Consumer(conf)
#     consumer.subscribe([args.topic])
#     return consumer

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Kafka Consumer')
    parser.add_argument('--bootstrap-servers', required=True, help='Kafka bootstrap servers')
    parser.add_argument('--topic', required=True, help='Kafka topic to consume')
    parser.add_argument('--group-id', default="None", help='Consumer group ID')
    parser.add_argument('--auto-offset-reset', default='earliest', choices=['earliest', 'latest'], help='Offset reset strategy')
    parser.add_argument('--enable-auto-commit', default=False, type=bool, help='Enable/disable auto-commit')
    parser.add_argument('--enable-sampling', default=True, type=bool, help='Enabla/disable sampling of data by 30%')
    parser.add_argument('--run-time', default= 120, type=int, help='Consumer closes after 120 seconds by default')
    parser.add_argument('--t1', default='ingetion_time',choices=['ingetion_time','producer_time'], help='configarable to ingestion time or CreateTime')
    parser.add_argument('--t2', default='produce_time',choices=['produce_time','consumer_time'], help='hh')
    
    args = parser.parse_args()
    
    count = 0
    sum = 0
    latency_array = []
    #consumer = create_consumer(args)
    conf = config_2.confi(args)
    consumer=Consumer(conf)
    consumer.subscribe([args.topic])

    pattern = r"\"ordertime\":(\d+)"
    
    start_time = time.time()
        
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                elapsed_time = time.time()-start_time
                #print(elapsed_time)
                if elapsed_time >= args.run_time:
                    consumer.close()
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('End of partition reached')
                else:
                    print('Error while consuming message: {}'.format(msg.error()))
            else:
                print('Received message: {}'.format(msg.value().decode('utf-8')))
                consumer.commit(asynchronous=False)

                start_time = time.time()
                
                count = count+1
                val = msg.value().decode('utf-8')
                if args.t1 == 'ingetion_time':
                    mat = re.search(pattern,val)
                    ordertime = mat.group(1)
                    ordertime=int(ordertime)
                 #   print('ordertime:',ordertime)
                else:
                    t = msg.timestamp()
                    t1 = int(t[1])
                  #  print(t1)
                if args.t2 == 'produce_time':
                    t = msg.timestamp()
                    t2 = int(t[1])
                   # print(t1)
                else:
                    t2 = time.time()*1000
                    #print(t2)
                latency = int(t2-t1)
                sum = sum +latency
                print("latency:",latency)
                # producer_latency.latency(latency,conf)
                latency_array.append(latency)


                

    except KeyboardInterrupt:
        pass
    finally:

        avg_latency = sum/count

        print("Total number of messages read:", count)

        print("average latency:",avg_latency)
        
        res = []

        if args.enable_sampling == True:
            n = int(len(latency_array)*0.3)
            for i in range(0,n):
                res.append(latency_array[i])
            print("sampling enabled and sample size is",n)
        else:
            res = latency_array
        
        quantiles = np.quantile(res, [.5, .9, .95, .99, .999])
        print("50 percerntile:",quantiles[0])
        print("90 percentile:",quantiles[1])
        print("95 percentile:",quantiles[2])
        print("99 percentile:",quantiles[3])
        print("99.9 percentile:",quantiles[4])




