from paho.mqtt import client as mqtt_client
import json
import time
from schema.agg_sch import AggSch
from source_file import DataSource
import config

def connect_mqtt(broker,port):
    print(f"Connected to {broker}:{port}")
    def on_connect(client,data,flags,rc):
        if rc==0:
            print(f"Connected to MQTT Broker ({broker}:{port})")
        else:
            print(f"Failed to connect {broker}:{port},err code: {rc}")
            exit(rc)
    client = mqtt_client.Client()
    client.on_connect = on_connect
    client.connect(broker,port)
    client.loop_start()
    return client

def publish(client,topic,datasource,delay):
    datasource.start_r()
    while True:
        time.sleep(delay)
        data= datasource.read()
        msg = AggSch().dumps(data)
        result = client.publish(topic,msg)
        status = result[0]
        if status == 0:
            pass
        else:
            print(f"Failed to send message to topic{topic}")

def run():
    print("PARMAETEEEEEEEEEEEEEEEEEERS:")
    print(config.MQTT_BROKER_HOST,config.MQTT_BROKER_PORT,config.MQTT_TOPIC,config.DELAY)
    client = connect_mqtt(config.MQTT_BROKER_HOST,config.MQTT_BROKER_PORT)
    src = DataSource([{"name":"data/accelerometer.csv","struct":[int,int,int]},
                      {"name":"data/gps.csv","struct":[float,float]}])
    publish(client,config.MQTT_TOPIC,src,config.DELAY)

if __name__ == '__main__':
    run()