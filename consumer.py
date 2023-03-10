def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf





from confluent_kafka import Consumer,Producer
import re
import numpy as np

props = read_ccloud_config("client.properties")
props["group.id"] = "python-group-1"
props["auto.offset.reset"] = "earliest"

pattern = r"\"ordertime\":(\d+)"

consumer = Consumer(props)
consumer.subscribe(["my-topic"])
count = 0
avg = 0
latency_arry = []
latency_array_quantile = []
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is not None and msg.error() is None:
             print("key = {key:12} value = {value:12}".format(key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
             val = msg.value().decode('utf-8')
             print(msg.timestamp())
             time = msg.timestamp()
             print(time[1])
             print(val)
             mat = re.search(pattern,val)
             print(mat)
             if mat:
                ordertime = mat.group(1)
                print(ordertime)
                ordertime=int(ordertime)
                time=int(time[1])
                latency = time-ordertime
                latency_arry.append(latency)
                latency_array_quantile.append(latency)
                print(latency)
                count =+ 1
                producer = Producer(read_ccloud_config("client.properties"))
                producer.produce("my-topic-latency", key=count.to_bytes((count.bit_length() + 7)// 8, byteorder='big'), value=latency.to_bytes((latency.bit_length() + 7)// 8, byteorder='big'))

                print(latency_arry)


                if len(latency_arry) == 10:
                     avg = (avg*count+sum(latency_arry))/count
                     print(avg)
                     latency_arry = []
                
                if len(latency_array_quantile) % 10 == 0:
                     quantiles = np.quantile(latency_array_quantile, [.5, .9, .95, .99, .999])
                     print(quantiles)
                     




                





except KeyboardInterrupt:
         pass
finally:
         consumer.close()
