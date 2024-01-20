from flask import Flask, jsonify, Response
from flask import request as flask_request
from flask import render_template
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
from geoalchemy2 import Geometry
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
from flask_apscheduler import APScheduler
from apscheduler.schedulers.background import BackgroundScheduler
import os
from dotenv import load_dotenv, find_dotenv
from pymongo import MongoClient
import asyncio
import aiohttp
from bson import json_util
import json


load_dotenv(find_dotenv())
password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://twright:{password}@windfarm.seyuy3o.mongodb.net/?retryWrites=true&w=majority"

cluster = MongoClient(connection_string)

db = cluster['Test']
collection = db['Test']


def insert_many_windspeeds(data):
    collection = db['Test']
    collection.insert_many(data)
    print(f"Windspeed data inserted into database at {datetime.now()}")

def gather_wind_data():
    print(f"Background Task started at {datetime.now()}")
    windfarm_df = pd.read_csv("Windfarm_WebScraped_DataV3.csv")
    now = datetime.now().replace(microsecond=0, second=0, minute=0)
    current_time = now.strftime('%Y-%m-%dT%H:%M')
    timestamp = datetime.today().replace(microsecond=0, second=0, minute=0)
    #Begin a session which keeps the same connection open for all api calls
    with requests.Session() as session:
        # Create a list of the wind speed results results using list comprehension 
        windspeeds = [gather_session_urls(session = session,name = row['Wind Farm Name'],lon = row['Longitude'],lat = row['Latitude'],current_time = current_time,timestamp = timestamp) for index, row in windfarm_df.iterrows()]
        for windspeed in windspeeds:
            print(windspeed)
        insert_many_windspeeds(windspeeds)

        



def gather_session_urls(session,name,lon,lat,current_time,timestamp):
    url = 'http://metwdb-openaccess.ichec.ie/metno-wdb2ts/locationforecast'
    qto = current_time
    qfrom = qto
    params= {"lat": lat, "long": lon, "from":qfrom, "to": qto}
    with session.get(url, params=params) as resp:
        try:
            response_text = resp.text
            root = ET.fromstring(response_text)
            wind_speed_element = root.find(".//windSpeed")
            # Extract the mps attribute value
            if wind_speed_element is not None:
                print(f"Wind Speed for {name} at lat={lat}, lon={lon}: {wind_speed_element.get('mps')}")
                windspeed = wind_speed_element.get("mps")
                data = {"metadata":{"Wind Farm Name":name},
                        "timestamp": timestamp,
                        "windspeed":windspeed}
                print(f"Background Task Executed at {datetime.now()}")
                return data
            else:
                print("Wind Speed element not found in the XML.")
                return None
        except Exception as e:
            print(f"Error processing wind data for lat={lat}, lon={lon}: {e}")
            return None
            
def test_scheduler():
    print("Hello World")

# set configuration values
class Config:
    SCHEDULER_API_ENABLED = True

# Initialize app
app = Flask(__name__)
app.config.from_object(Config())
basedir = os.path.abspath(os.path.dirname(__file__))


# Initialize scheduler
scheduler = BackgroundScheduler()
# Add the task to the scheduler to run at the start of every hour
scheduler.add_job(gather_wind_data, 'cron', hour = "*", minute="0")

# Add a test task to the scheduler to run at the start of every hour
scheduler.add_job(test_scheduler, 'cron', minute="*")

# Start the scheduler
scheduler.start()

@app.route('/')
def index():
    #Read Windfarm data into a pandas dataframe
    windfarm_data = pd.read_csv("Windfarm_WebScraped_Datav3.csv")
    print(windfarm_data.to_json(orient="records"))
    return render_template('index.html',windfarm_data = windfarm_data.to_json(orient="records"))

@app.route("/windfarm_details", methods = ['POST'])
def windfarm_details():
    #Read Windfarm data into a pandas dataframe
    windfarm_data = pd.read_csv("Windfarm_WebScraped_Datav3.csv")
    if flask_request.method == 'POST':
        windfarm_name = flask_request.json['Windfarm']
        windfarm_details= windfarm_data.loc[windfarm_data['Wind Farm Name']== windfarm_name ]
        # Convert DataFrame to JSON string
        json_data = windfarm_details.to_json(orient='records')
        print(json_data)
    # Return JSON response
    return json.loads(json_data)

@app.route("/choropleth")
def choropleth():
    geojson_path = 'Counties_-_National_Statutory_Boundaries_-_2019_-_Generalised_20m.geojson'
    print(geojson_path)
    try:
        with open(geojson_path, 'r', encoding='utf-8') as file:
            geojson_data = file.read()
            return Response(geojson_data, content_type='application/json')
    except FileNotFoundError:
        return jsonify({"error": "GeoJSON file not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/lookup", methods=['POST'])
def lookup():
    if flask_request.method == 'POST':
        windfarm_name = flask_request.json['Windfarm']
        print(windfarm_name)
        #This is a MongoDB Query on a collection to check the data for a Wind farm
        windfarm_query = collection.find({"metadata":{"Wind Farm Name": windfarm_name}})
        return_query = []
        for row in windfarm_query:
            return_query.append(row)
        print(json.loads(json_util.dumps(return_query)) ) 
    return json.loads(json_util.dumps(return_query))

@app.route("/wind")
def wind():
    return render_template('wind.html')


if __name__ == '__main__':
    app.run(debug=True)