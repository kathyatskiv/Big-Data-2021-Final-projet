from flask import Flask, request, jsonify
from flask_restful import Api, Resource
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

app = Flask(__name__)

auth_provider = PlainTextAuthProvider(username='cassandra', password='qjkBX3TwWtvm')
cluster = Cluster(['35.230.169.127'], port=9042, auth_provider=auth_provider)
session = cluster.connect('meetups')

# Return the list of all the countries for which the events were created.
@app.route("/all_countries", methods=['GET'])
def get_all_countries():
    rows = session.execute(
        """
        SELECT DISTINCT country FROM events_by_country_and_city;
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
        FROM events_by_country_and_city
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
        SELECT event_name, event_time, group_name, city, country
        FROM events_by_id
        WHERE event_id='%s';
        """ % event_id
    )

    events = []
    for row in rows:
        events.append({
            'event_name' : row.event_name,
            'event_time' : row.event_time,
            'group_name' : row.group_name,
            'city' : row.city,
            'country' : row.country
        })


    return jsonify(events)

if __name__ == "__main__" :
    app.run(debug=True, port=5001,host="127.0.0.1")