from os import error
import random
from random import randint
import argparse
from datetime import timedelta, datetime as dt
from kafka.admin import KafkaAdminClient, NewTopic
#from bson import json_util
import json
import os
from kafka import KafkaProducer
import decimal
#from kafka.admin import KafkaAdminClient, NewTopic
import threading
import sys
import time

lock = threading.Lock()

volatility = 1  # .001

device_id=[]
group_id=[]

def generate_devices(numberofdevices):
    for i in range(0,numberofdevices,1):
        device_id.append('device_' + str(i))
        group_id.append(i%10)
        print('creating device_' + str(i) + ' in group ' + str(i%10) )

#this function is used to randomly increase/decrease the value, tweak the random.uniform line for more dramatic changes
def getvalue(old_value):
    change_percent = volatility * \
        random.uniform(0.0, .001)  # 001 - flat .01 more
    change_amount = old_value * change_percent
    if bool(random.getrandbits(1)):
        new_value = old_value + change_amount
    else:
        new_value = old_value - change_amount
    return round(new_value, 2)

def checkkafkaconnection():
    try:
        print('\nAttempting to create topic' + args.topic + ' on ' + args.bootstrap)
        admin_client = KafkaAdminClient(bootstrap_servers=args.bootstrap, client_id='solargenclient')

      #  consumer = kafka.KafkaConsumer(group_id='stockgenclient', bootstrap_servers=[args.bootstrap])
        topic_list = []
        topic_list.append(NewTopic(name=args.topic, num_partitions=1, replication_factor=1))
        admin_client.create_topics(new_topics=topic_list, validate_only=False)

    except Exception as e:
        print('%s - skipping topic creation' % e )
        if str(e)=='NoBrokersAvailable': return False
        return True
        
def publish_message(kafka_producer, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        kafka_producer.send(topic_name, key=key_bytes, value=value_bytes)
        kafka_producer.flush()
    except Exception as ex:
        print('Error writing to Kafka - %s' % str(ex))

def printstats(counter,starttime):
    b=dt.now()
    s=(b-starttime).total_seconds()
    t=round(decimal.Decimal(counter)/decimal.Decimal(s),2)
    print('Generated %s records. Rate %s/s' % (counter,t), end='\r')

def main():
    global args
    global MONGO_URI

    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--devices", type=int, default=250, help="number of devices")
#    parser.add_argument("-k","--kafka",help="Write to Kafka Topic",action="store_true")  In future may support writing to other destinations
    parser.add_argument("-kb","--bootstrap",type=str, default='localhost:9092', help="Provide the bootstrap server")
    parser.add_argument("-kt","--topic",type=str, default='solar', help="Provide the default topic name") 
    parser.add_argument("-x","--duration",type=int, default=0, help="Number of minutes of data to generate (default 0 - forever)")

    args = parser.parse_args()

    if args.devices:
        if args.devices < 1:
            args.devices = 1

    threads = []

    generate_devices(args.devices)
    
    for i in range(0, 1): # parallel threads
        t = threading.Thread(target=kafkaworker, args=[int(i), int(args.devices)])
        threads.append(t)
    for x in threads:
        x.start()
    for y in threads:
        y.join()

def kafkaworker(workerthread, numofgroups):
    try:
        #Create an initial value for each device
        last_watts=[]
        last_amps=[]
        last_temp=[]
        for i in range(0,numofgroups):
            last_watts.append(round(random.uniform(200, 250), 0))
            last_amps.append(round(random.uniform(2, 3), 2))
            last_temp.append(round(random.uniform(5,25),0))

        #Wait until MongoDB Server is online and ready for data
        while True:
            print('Checking Kafka Connection')
            if checkkafkaconnection()==False:
                print('Problem connecting to Kafka, sleeping 10 seconds')
                time.sleep(10)
            else:
                break
        print('Successfully connected to Kafka')

        starttime=dt.now() # used to cal avg write speed
        txtime = dt.now() # use for ficticous timestamps in events
        txtime_end=txtime+timedelta(minutes=args.duration)

        print('Data Generation Summary:\n{:<12} {:<12}\n{:<12} {:<12}\n{:<12} {:<12}'.format('# devices',args.devices,'Bootstrap Server',args.bootstrap,'Topic',args.topic))
        print('\n{:<12} {:<12}'.format('Start time',txtime.strftime('%Y-%m-%d %H:%M:%S')))
        if args.duration:
            print('{:<12} {:<12}\n'.format('End time',txtime_end.strftime('%Y-%m-%d %H:%M:%S')))
        else:
            print('No end time \n\n')
        
        kafka_producer = KafkaProducer(bootstrap_servers=[args.bootstrap], api_version=(0, 10))

        counter=0
        bContinue=True
        while bContinue:
            for i in range(0,args.devices):
                #Get the last value of this particular security
                w = getvalue(last_watts[i])
                last_watts[i] = w
                a = getvalue(last_amps[i])
                last_amps[i] = a
                t = getvalue(last_temp[i])
                last_temp[i] = t
                try:
                    solar={'device_id' : device_id[i], 'group_id': group_id[i],'timestamp': txtime.strftime('%Y-%m-%dT%H:%M:%SZ'),'maxwatts':250,'obs':[{'watts':w,'amps':a,'temp':t}]}
                    publish_message(kafka_producer=kafka_producer,topic_name=args.topic,key=device_id[i],value=json.dumps(solar))
                    counter=counter+1
                    if counter%100==0:
                      if args.duration>0:
                        print('Generated ' + str(counter) + ' samples ({0:.0%})'.format(counter/(args.devices*args.duration*60)),end="\r")
                      else:
                        print('Generated ' + str(counter), end="\r")
                    if args.duration>0:
                      if txtime > txtime_end:
                        bContinue=False
                        continue
                except Exception as e:
                    print("error: " + str(e))
            txtime+=timedelta(seconds=1)
        duration=txtime - dt.now()
        print('\nFinished - ' + str(duration).split('.')[0])
        if kafka_producer is not None:
            kafka_producer.close()
    except:
        print('Unexpected error:', sys.exc_info()[0])
        raise

if __name__ == '__main__':
    main()
