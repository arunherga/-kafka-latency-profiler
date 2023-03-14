from confluent_kafka import Producer

def latency(latency,config):

    producer=Producer(config)
    producer.produce("latency-01", value=latency.to_bytes((latency.bit_length() + 7)// 8, byteorder='big'))
    producer.flush()
    