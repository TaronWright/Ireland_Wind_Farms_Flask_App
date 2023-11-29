from flask import Flask
from flask import request as flask_request
from flask import render_template
from folium.plugins import MarkerCluster
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
from geoalchemy2 import Geometry
import csv
from dataclasses import dataclass
import folium
import geopandas as gpd
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
from flask_apscheduler import APScheduler
import os
from requests import request
import jsonify

# set configuration values
class Config:
    SCHEDULER_API_ENABLED = True

# Initialize app
app = Flask(__name__)
app.debug = True
app.config.from_object(Config())
basedir = os.path.abspath(os.path.dirname(__file__))

# Database
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'db,sqlite')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Init database
db = SQLAlchemy(app)

# Product Class/Model
@dataclass
class WindfarmWindSpeed(db.Model):
    id:int = db.Column(db.Integer,primary_key = True)
    timestamp:datetime = db.Column(db.DateTime)
    name:str = db.Column(db.String(255))
    windspeed:int = db.Column(db.Float)
    windpower:int = db.Column(db.Float)

    def __init__(self,timestamp,name,windspeed,windpower):
        self.timestamp = timestamp
        self.name = name
        self.windspeed = windspeed
        self.windpower = windpower

# Create database if one does not already exist  
with app.app_context():     
    db.create_all()

# initialize scheduler
scheduler = APScheduler()
# if you don't wanna use a config, you can set options here:
# scheduler.api_enabled = True
scheduler.init_app(app)
scheduler.start()

# # interval example
@scheduler.task('interval', id='do_scrape_windspeed_data', hours= 1, misfire_grace_time=900)
def scrape_windspeed_data():
    df = pd.read_csv("Windfarm_WebScraped_DataV3.csv")
    now = datetime.now().replace(microsecond=0, second=0, minute=0)
    current_time = now.strftime('%Y-%m-%dT%H:%M')
    database_timestamp = datetime.strptime(current_time,"%Y-%m-%dT%H:%M")
    url = 'http://metwdb-openaccess.ichec.ie/metno-wdb2ts/locationforecast'

    windspeeds= []

    for index,row in df.iterrows():
        lat = str(row['Latitude'])
        long = str(row['Longitude'])
        qfrom = current_time
        qto = current_time
        query= {"lat": lat, "long": long, "from":qfrom, "to": qto}
        response = requests.get(url, params = query)
        root = ET.fromstring(response.content)
        x = [time.attrib['from'] for time in root.iter('time')]
        x = [item for item in x[::2]]
        x = [datetime.strptime(time,"%Y-%m-%dT%H:%M:%SZ") for time in x]
        windSpeed = [windSpeed.attrib['mps'] for windSpeed in root.iter('windSpeed')]
        windSpeed = [float(speed) for speed in windSpeed]
        
        cutin_speed = row['Cut-in wind speed']
        cutoff_speed = row['Cut-off wind speed']
        rotor_radius = row['Rotor diameter']
        number_of_turbines = row['Number of Turbines']
        if windSpeed:
            if cutin_speed <= windSpeed[0] <= cutoff_speed:
                wind_power = (0.5*1.225*3.14*(rotor_radius**2)*(windSpeed[0]**3)*0.4)
            else:
                wind_power = 0
        new_row = WindfarmWindSpeed(database_timestamp,row['Wind Farm Name'],windSpeed[0],wind_power)
        db.session.add(new_row)
        db.session.commit()
    print('Job 1 executed')

#Uncomment if function needs to be run on app startup to get more data
# with app.app_context():
#     scrape_windspeed_data()


windfarms_data = pd.read_csv("Windfarm_WebScraped_DataV3.csv")

@app.route('/')
def index():
    #Read Windfarm data into a pandas dataframe
    windfarm_data = pd.read_csv("Windfarm_WebScraped_Datav3.csv")
    print(windfarm_data.to_json(orient="records"))
    return render_template('index.html',windfarm_data = windfarm_data.to_json(orient="records"))


@app.route("/lookup", methods=['POST'])
def lookup():
    if flask_request.method == 'POST':
        windfarm_name = flask_request.json['Windfarm']
        print(windfarm_name)
        windfarm_query = WindfarmWindSpeed.query.filter_by(name=windfarm_name).all()
        for row in windfarm_query:
            print(row.timestamp)
            print(row.windspeed)
            print(row.windpower)
    return windfarm_query


@app.route("/windspeed")
def windspeed():
    df = pd.read_csv("Windfarm_WebScraped_DataV3.csv")
    now = datetime.now().replace(microsecond=0, second=0, minute=0)
    current_time = now.strftime('%Y-%m-%dT%H:%M')
    database_timestamp = datetime.strptime(current_time,"%Y-%m-%dT%H:%M")
    url = 'http://metwdb-openaccess.ichec.ie/metno-wdb2ts/locationforecast'

    windspeeds= []

    for index,row in df.iterrows():
        lat = str(row['Latitude'])
        long = str(row['Longitude'])
        qfrom = current_time
        qto = current_time
        query= {"lat": lat, "long": long, "from":qfrom, "to": qto}
        response = requests.get(url, params = query)
        root = ET.fromstring(response.content)
        x = [time.attrib['from'] for time in root.iter('time')]
        x = [item for item in x[::2]]
        x = [datetime.strptime(time,"%Y-%m-%dT%H:%M:%SZ") for time in x]
        windSpeed = [windSpeed.attrib['mps'] for windSpeed in root.iter('windSpeed')]
        windSpeed = [float(speed) for speed in windSpeed]
        new_row = WindfarmWindSpeed(database_timestamp,row['Wind Farm Name'],windSpeed[0])
        db.session.add(new_row)
        db.session.commit()
    return render_template("windspeed.html", windspeeds = windspeeds)

if __name__ == '__main__':
    app.run(debug=True)