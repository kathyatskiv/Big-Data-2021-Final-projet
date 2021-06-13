import sys
sys.path.append("../public")
from config import cassandra_username, cassandra_password, cassandra_host

from flask import Flask, request, jsonify
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import requests

app = Flask(__name__)

auth_provider = PlainTextAuthProvider(username=cassandra_username, password=cassandra_password)
cluster = Cluster([cassandra_host], port=9042, auth_provider=auth_provider)
session = cluster.connect('meetups')

# Return the list of all the countries for which the events were created.
@app.route("/all_countries", methods=['GET'])
def get_all_countries():
    rows = session.execute(
        """
        SELECT DISTINCT country FROM cities_by_country;
        """
    )

    countries = []
    for row in rows:
        countries.append(row.country)


    return jsonify(countries)

# Return the list of the cities for the specified country where at least one event was created.
@app.route("/all_cities_by_country/<country>", methods=['GET'])
def get_all_cities_by_country(country):
    rows = session.execute(
        """
        SELECT country, city 
        FROM cities_by_country
        WHERE country='%s';
        """ % country
    )
    
    cities = []
    for row in rows:
        cities.append(row.city)


    return jsonify(cities)

# Given the event id, return the following details:
# a) event name
# b) event time
# c) the list of the topics
# d) the group name
# e) the city and the country of the event
@app.route("/event_by_id/<event_id>", methods=['GET'])
def get_event_by(event_id):
    rows = session.execute(
        """
        SELECT event_name, event_time, group_name, city, country, group_topics
        FROM events_by_id
        WHERE event_id='%s';
        """ % event_id
    )

    events = []
    for row in rows:
        events.append({
            'event_name' : row.event_name,
            'event_time' : row.event_time,
            'group_topics' : row.group_topics,
            'group_name' : row.group_name,
            'city' : row.city,
            'country' : row.country
        })

    return jsonify(*events)

# Return the list of the groups which have created events in the specified city. It should contain the following details:
# a) City name
# b) Group name
# c) Group id
@app.route("/groups_by_city/<city>", methods=['GET'])
def get_groups_by_city(city):
    rows = session.execute(
        """
        SELECT city, group_name, group_id
        FROM groups_by_city
        WHERE city='%s';
        """ % city
    )

    groups = []
    for row in rows:
        groups.append({
            'city' : row.city,
            'group_name' : row.group_name,
            'group_id' : row.group_id
        })

    return jsonify(groups)

# Return all the events that were created by the specified group (group id will be the input parameter). Each event in the list should have the format as in the API #3.
@app.route("/events_by_group/<group_id>", methods=['GET'])
def get_events_by_group(group_id):
    rows = session.execute(
        """
        SELECT event_name, event_time, group_name, city, country, group_topics
        FROM events_by_group
        WHERE group_id=%s
        ALLOW FILTERING;
        """ % group_id
    )

    events = []
    for row in rows:
        events.append({
            'event_name' : row.event_name,
            'event_time' : row.event_time,
            'group_topics' : row.group_topics,
            'group_name' : row.group_name,
            'city' : row.city,
            'country' : row.country
        })

    return jsonify(events)

# Return the statistics with the number of newly created events per each country for the last 6 full hours, excluding the last hour. The report should be in a format : [{country1: number1}, {country2: number2},..]. 
@app.route("/new_events_amount_by_country/", methods=['GET'])
def get_new_events_amount_by_country():
    url = 'https://storage.googleapis.com/ucu-bigata-project/reports/query1/part-00000-ae686376-83a7-452a-a619-fb3ef6a39540-c000.json'
    f = requests.get(url, allow_redirects=True)
    
    for line in f.iter_lines():
        if line:
            decoded_line = line.decode('utf-8')
            countries_statistics = json.loads(decoded_line)
    
    return jsonify(countries_statistics)

# Return the statistics containing the information about which groups posted the events at each US state in the last 3 full hours, excluding the last hour. The names of the states should be full, not only the state code.
@app.route("/us_active_groups_by_state/", methods=['GET'])
def get_us_active_groups_by_state():
    url = 'https://storage.googleapis.com/ucu-bigata-project/reports/query2/part-00000-50589504-24c6-43d0-a9ec-731da31cbb7a-c000.json'
    f = requests.get(url, allow_redirects=True)
    
    for line in f.iter_lines():
        if line:
            decoded_line = line.decode('utf-8')
            us_states_statistics = json.loads(decoded_line)
    
    return jsonify(us_states_statistics)

# The most popular topic of the events for each country posted in the last 6 hours, excluding the last hour. The popularity is calculated based on the number of occurrences topic has amongst all the topics in all the events created in that country during the specified period. 
@app.route("/most_popular_topic_by_country/", methods=['GET'])
def get_most_popular_topic_by_country():
    url = 'https://storage.googleapis.com/ucu-bigata-project/reports/query3/part-00000-0518cbb2-56d5-44e0-a7de-2f546b1b6562-c000.json'
    f = requests.get(url, allow_redirects=True)
    
    for line in f.iter_lines():
        if line:
            decoded_line = line.decode('utf-8')
            most_populsr_topic_statistics = json.loads(decoded_line)
    
    return jsonify(most_populsr_topic_statistics)


if __name__ == "__main__" :
    app.run(debug=True)