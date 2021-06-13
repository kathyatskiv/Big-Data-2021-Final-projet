from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import datetime
import csv
from config import HOST_1, HOST_2, HOST_3


if __name__ == "__main__":
    consumer = KafkaConsumer('all_events', group_id='batch-proc',
                             bootstrap_servers=[HOST_1, HOST_2, HOST_3],
                             auto_offset_reset='earliest',
                             enable_auto_commit=True)
    count = 0
    with open('events_file.csv', mode='w', encoding='utf-8') as employee_file:
        employee_writer = csv.writer(employee_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for msg in consumer:
            count += 1
            value = json.loads(msg.value)
            event_id = value["event"]["event_id"]
            group_name = value["group"]["group_name"].replace('"', '')
            group_country = value["group"]["group_country"]
            group_state = value["group"].get("group_state")
            if group_state is not None:
                group_state.encode('utf-8')
            group_topics = ', '.join(list(map(lambda x: x["topic_name"].replace('"', ''), value["group"]["group_topics"])))

            row = [event_id, group_name, group_country, group_state, group_topics, msg.timestamp]
            # print(str(row).encode('utf-8'))
            if count % 1000 == 0:
                print(count)
            employee_writer.writerow(row)