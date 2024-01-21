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
import math


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
        windspeeds = [gather_session_urls(session = session,name = row['Wind Farm Name'],lon = row['Longitude'],lat = row['Latitude'],rotordiameter = row['Rotor diameter'], numberofturbines = row['Number of Turbines'] , cutinspeed=row['Cut-in wind speed'], cutoutspeed=row['Cut-off wind speed'], ratedspeed=row['Rated wind speed'], current_time = current_time,timestamp = timestamp) for index, row in windfarm_df.iterrows()]
        for windspeed in windspeeds:
            print(windspeed)
        insert_many_windspeeds(windspeeds)

def windpower(windspeed: float,rotordiameter: float, cutinspeed: float, cutoutspeed: float, ratedspeed:float):
     # Initiate constants:
    air_density = 1.225  # Standard air density at sea level in kg/m^3
    power_coefficient = 0.593  # Ideal power coefficient (Betz limit)

    #Swept area
    swept_area = math.pi * rotordiameter**2
    #Calculate the power produced by a wind turbine.

    # Power produced by the wind turbine in watts.
    #If the windspeed is less than the cutin wind speed, the turbine does not generate power
    if windspeed < cutinspeed:
        windfarmpower = 0
    #If the windspeed is greater than the rated wind speed wind speed then turbine generates power at it's rated wind speed
    elif windspeed >= ratedspeed:
        windfarmpower =  0.5 * air_density * swept_area * ratedspeed**3 * power_coefficient
    #If the windspeed is greater than the cutout wind speed, the turbine does not generate power for safety reasons
    elif windspeed >= cutoutspeed:
        windfarmpower =  0
    #If the windspeed is between the cutin and cutoff then turbine generates a power curve
    else:
        windfarmpower = 0.5 * air_density * swept_area * windspeed**3 * power_coefficient
    return windfarmpower




def gather_session_urls(session,name,lon,lat, rotordiameter, numberofturbines,cutinspeed, cutoutspeed, ratedspeed, current_time,timestamp):
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
                if windspeed:
                    #Calculate the windpower generated by the windfarm at this speed
                    windfarmpower = numberofturbines*windpower(windspeed = float(windspeed), rotordiameter = float(rotordiameter), cutinspeed= float(cutinspeed), cutoutspeed= float(cutoutspeed), ratedspeed= float(ratedspeed))
                    print(windfarmpower)
                else:
                    print("Windspeed value was empty")
                    windfarmpower = 0
                data = {"metadata":{"Wind Farm Name":name},
                        "timestamp": timestamp,
                        "windspeed":windspeed,
                        "windpower":windfarmpower}
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
scheduler.add_job(gather_wind_data, 'cron', hour = "*", minute="44")

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
        #This is a MongoDB Query on a collection to check the data for a Wind farm
        windfarm_query = collection.find({"metadata":{"Wind Farm Name": windfarm_name}})
        return_query = []
        for row in windfarm_query:
            return_query.append(row)
    return json.loads(json_util.dumps(return_query))

@app.route("/windpower")
def aggregate_windpower():
    #This is a MongoDB Query on a collection to check the data for a Wind farm
    aggregate_windpower = collection.aggregate([
        {
            "$group":
            {"_id": "$metadata.Wind Farm Name", "windpower": {"$sum": "$windpower"}}
        }
        ])
    print(json_util.dumps(aggregate_windpower))
    return jsonify(json_util.dumps(aggregate_windpower))

@app.route("/wind")
def wind():
    return render_template('wind.html')


if __name__ == '__main__':
    app.run(debug=True)