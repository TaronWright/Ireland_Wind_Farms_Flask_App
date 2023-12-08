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
from requests import request
from dotenv import load_dotenv, find_dotenv
from pymongo import MongoClient
import asyncio
import aiohttp

load_dotenv(find_dotenv())
password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://twright:{password}@windfarm.seyuy3o.mongodb.net/?retryWrites=true&w=majority"

cluster = MongoClient(connection_string)

db = cluster['Test']

print(datetime.utcnow())

def insert_many_windspeeds(data):
    collection = db['Test']
    collection.insert_many(data)

async def gather_wind_data():
    windfarm_df = pd.read_csv("Windfarm_WebScraped_DataV3.csv")
    now = datetime.now().replace(microsecond=0, second=0, minute=0)
    current_time = now.strftime('%Y-%m-%dT%H:%M')
    timestamp = datetime.today().replace(microsecond=0, second=0, minute=0)
    async with aiohttp.ClientSession() as session:  
        semaphore = asyncio.Semaphore(7)  # Limit concurrency to 5
        async with asyncio.TaskGroup() as task_group:
                # Create a list of the task coroutine results using list comprehension 
                windspeeds = [task_group.create_task(gather_session_urls(semaphore,session,name = row['Wind Farm Name'],lon = row['Longitude'],lat = row['Latitude'],current_time = current_time,timestamp = timestamp)) for index, row in windfarm_df.iterrows()]
        windspeeds = [windspeed.result() for windspeed in windspeeds]
        insert_many_windspeeds(windspeeds)
        



async def gather_session_urls(semaphore,session,name,lon,lat,current_time,timestamp):
    url = 'http://metwdb-openaccess.ichec.ie/metno-wdb2ts/locationforecast'
    qto = current_time
    qfrom = qto
    params= {"lat": lat, "long": lon, "from":qfrom, "to": qto}  
    async with semaphore:  # Enforce concurrency limit
        async with session.get(url, params=params) as resp:
            try:
                response_text = await resp.text()
                root = ET.fromstring(response_text)
                wind_speed_element = root.find(".//windSpeed")
                # Extract the mps attribute value
                if wind_speed_element is not None:
                    print(f"Wind Speed for {name} at lat={lat}, lon={lon}: {wind_speed_element.get('mps')}")
                    windspeed = wind_speed_element.get("mps")
                    data = {"metadata":{"Wind Farm Name":name},
                            "timestamp": timestamp,
                            "windspeed":windspeed}
                    return data
                else:
                    print("Wind Speed element not found in the XML.")
                    return None
            except Exception as e:
                print(f"Error processing wind data for lat={lat}, lon={lon}: {e}")
                return None

# # set configuration values
# class Config:
#     SCHEDULER_API_ENABLED = True

# # Initialize app
# app = Flask(__name__)
# app.debug = True
# app.config.from_object(Config())
# basedir = os.path.abspath(os.path.dirname(__file__))

# # Database
# app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'db,sqlite')
# app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# # Init database
# db = SQLAlchemy(app)

# # Product Class/Model
# @dataclass
# class WindfarmWindSpeed(db.Model):
#     id:int = db.Column(db.Integer,primary_key = True)
#     timestamp:datetime = db.Column(db.DateTime)
#     name:str = db.Column(db.String(255))
#     windspeed:int = db.Column(db.Float)
#     windpower:int = db.Column(db.Float)

#     def __init__(self,timestamp,name,windspeed,windpower):
#         self.timestamp = timestamp
#         self.name = name
#         self.windspeed = windspeed
#         self.windpower = windpower

# # Create database if one does not already exist  
# with app.app_context():     
#     db.create_all()


# # Scheduled task to be run
# def scheduled_task():
#     with app.app_context():
#         df = pd.read_csv("Windfarm_WebScraped_DataV3.csv")
#         now = datetime.now().replace(microsecond=0, second=0, minute=0)
#         current_time = now.strftime('%Y-%m-%dT%H:%M')
#         database_timestamp = datetime.strptime(current_time,"%Y-%m-%dT%H:%M")
#         url = 'http://metwdb-openaccess.ichec.ie/metno-wdb2ts/locationforecast'
#         windspeeds= []
#         for index,row in df.iterrows():
#             lat = str(row['Latitude'])
#             long = str(row['Longitude'])
#             qfrom = current_time
#             qto = current_time
#             query= {"lat": lat, "long": long, "from":qfrom, "to": qto}
#             response = requests.get(url, params = query)
#             root = ET.fromstring(response.content)
#             x = [time.attrib['from'] for time in root.iter('time')]
#             x = [item for item in x[::2]]
#             x = [datetime.strptime(time,"%Y-%m-%dT%H:%M:%SZ") for time in x]
#             windSpeed = [windSpeed.attrib['mps'] for windSpeed in root.iter('windSpeed')]
#             windSpeed = [float(speed) for speed in windSpeed]
#             cutin_speed = row['Cut-in wind speed']
#             cutoff_speed = row['Cut-off wind speed']
#             rotor_radius = row['Rotor diameter']
#             number_of_turbines = row['Number of Turbines']
#             if windSpeed:
#                 if cutin_speed <= windSpeed[0] <= cutoff_speed:
#                     wind_power = (0.5*1.225*3.14*(rotor_radius**2)*(windSpeed[0]**3)*0.4)
#                 else:
#                     wind_power = 0
#             new_row = WindfarmWindSpeed(database_timestamp,row['Wind Farm Name'],windSpeed[0],wind_power)
#             db.session.add(new_row)
#             db.session.commit()
#     print('Windspeeds for each windfarm data scraped')

# # Initialize scheduler
# scheduler = BackgroundScheduler()
# # Add the task to the scheduler to run at the start of every hour
# scheduler.add_job(scheduled_task, 'cron', hour='*', minute=5)
# # Start the scheduler
# scheduler.start()


# #Uncomment if function needs to be run on app startup to get more data
# # with app.app_context():
# #     scrape_windspeed_data()


# @app.route('/data')
# def data():
#     return render_template('data.html')

# windfarms_data = pd.read_csv("Windfarm_WebScraped_DataV3.csv")

# @app.route('/')
# def index():
#     #Read Windfarm data into a pandas dataframe
#     windfarm_data = pd.read_csv("Windfarm_WebScraped_Datav3.csv")
#     print(windfarm_data.to_json(orient="records"))
#     return render_template('index.html',windfarm_data = windfarm_data.to_json(orient="records"))

# @app.route("/choropleth")
# def choropleth():
#     geojson_path = 'Counties_-_National_Statutory_Boundaries_-_2019_-_Generalised_20m.geojson'
#     print(geojson_path)

#     try:
#         with open(geojson_path, 'r', encoding='utf-8') as file:
#             geojson_data = file.read()
#             return Response(geojson_data, content_type='application/json')
#     except FileNotFoundError:
#         return jsonify({"error": "GeoJSON file not found"}), 404
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500

# @app.route("/lookup", methods=['POST'])
# def lookup():
#     if flask_request.method == 'POST':
#         windfarm_name = flask_request.json['Windfarm']
#         print(windfarm_name)
#         windfarm_query = WindfarmWindSpeed.query.filter_by(name=windfarm_name).all()
#         for row in windfarm_query:
#             print(row.timestamp)
#             print(row.windspeed)
#             print(row.windpower)
#     return windfarm_query


if __name__ == '__main__':
    asyncio.run(gather_wind_data())
    # app.run(debug=True)