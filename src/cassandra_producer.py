import sys
sys.path.append("../public")
from config import cassandra_username, cassandra_password, cassandra_host, HOST_1, HOST_2, HOST_3

import json
import requests
import cassandra
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from kafka import KafkaConsumer

auth_provider = PlainTextAuthProvider(username=cassandra_username, password=cassandra_password)
cluster = Cluster([cassandra_host], port=9042, auth_provider=auth_provider)
session = cluster.connect('meetups')

consumer = KafkaConsumer(
     bootstrap_servers=[HOST_1, HOST_2, HOST_3],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     group_id='cassandra-consumer')

consumer.subscribe(topics=['all_events'])

for message in consumer:
    message = message.value.decode('utf-8')
    data = json.loads(message)

        
    event = data["event"]
    group = data["group"]

    topics = []
    for topic in group["group_topics"]:
        topics.append(topic["topic_name"])
    
    try:
        session.execute(
            """
            INSERT INTO cities_by_country (country, city)
            VALUES (%s, %s)
            IF NOT EXISTS
            """, 
            (group["group_country"], group["group_city"])
        )
    except cassandra.InvalidRequest as err:
        print(err)
        pass


    try:
        session.execute(
            """
            INSERT INTO events_by_id (country, city, event_id, event_time, event_name, group_name, group_topics)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            IF NOT EXISTS
            """, 
            (group["group_country"], group["group_city"], event["event_id"], event["time"], event["event_name"], group["group_name"], topics)
        )
    except cassandra.InvalidRequest as err:
        print(err)
        pass

    try:
        session.execute(
            """
            INSERT INTO groups_by_city (city, group_id, group_name)
            VALUES (%s, %s, %s)
            IF NOT EXISTS
            """, 
            (group["group_city"], group["group_id"], group["group_name"])
        )
    except cassandra.InvalidRequest as err:
        print(err)
        pass

    try:
        session.execute(
            """
            INSERT INTO events_by_group (country, city, event_id, event_time, event_name, group_name, group_id, group_topics)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            IF NOT EXISTS
            """, 
            (group["group_country"], group["group_city"], event["event_id"], event["time"], event["event_name"], group["group_name"], group["group_id"], topics)
        )
    except cassandra.InvalidRequest as err:
        print(err)
        pass
        

    print(event["event_id"],  " successfully loaded")
    