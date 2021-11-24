import json
from typing import Collection
from kafka import KafkaConsumer, consumer
from json import loads
import collections
import datetime
import psycopg2


if __name__ == "__main__":

    consumer = KafkaConsumer (
            "registered_user",
            bootstrap_servers = "localhost:9092",
            auto_offset_reset = "latest",
            group_id ="consumer-group-a",
            enable_auto_commit=True,
            value_deserializer = lambda x:loads(x.decode('utf-8'))

            )

    conn= psycopg2.connect(database ='test',user='postgres', host='localhost',password= 'password', port= 5432)
    cursor = conn.cursor()

    while True:
    
        #msg_pack = consumer.poll(timeout_ms=500)
        msg_pack = consumer.poll(40)

        for tp, messages in msg_pack.items():
            for message in messages:
                message = message.value
                
                insert_value = list(message.values())
                #for i in range(0,len(insert_value)):
                print(insert_value)
                name1 =insert_value[0]
                #print (name1)
                country1 =insert_value[1]
                #print(country1)
                created_at1 = insert_value[2]
                #print(created_at1)
                insert_script = '''INSERT INTO producer_data(name,country,created_at) VALUES(%s,%s,%s) 
                                        '''
                   
                cursor.execute(insert_script,(str(name1),str(country1),str(created_at1)))
                conn.commit()
                    
            conn.close()