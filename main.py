from flask import Flask
from flask import render_template
from folium.plugins import MarkerCluster
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
from geoalchemy2 import Geometry
import csv
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
class WindfarmWindSpeed(db.Model):
    id = db.Column(db.Integer,primary_key = True)
    timestamp = db.Column(db.DateTime)
    name = db.Column(db.String(255))
    windspeed = db.Column(db.Float)

    def __init__(self,timestamp,name,windspeed):
        self.timestamp = timestamp
        self.name = name
        self.windspeed = windspeed


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
        new_row = WindfarmWindSpeed(database_timestamp,row['Wind Farm Name'],windSpeed[0])
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


@app.route("/map")
def map():
    #Read the geojson file into a geodataframe
    geo_df = gpd.read_file("Counties_-_National_Statutory_Boundaries_-_2019_-_Generalised_20m.geojson")

    #Create a folium map object
    m = folium.Map(location = [53.4287,-8.3321], zoom_start= 7 , dragging = False)

    #Create a MarkerCluster object
    # marker_cluster = MarkerCluster().add_to(m)

    #Creat a Choropleth layer and add it to the map
    folium.Choropleth(
    geo_data=geo_df,
    fill_opacity=0.3,
    line_weight=2,
    ).add_to(m)

    #Read Windfarm data into a pandas dataframe
    df = pd.read_csv("Windfarm_WebScraped_Datav3.csv")

    df_dict = df.to_dict(orient="records")

    #Create a geopandas dataframe with a geometry column from the Long and Lat columns in the dataframe
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['Longitude'], df['Latitude']), crs="EPSG:4326")

     #Create the geojson layer with tooltips and popups
    folium.GeoJson(
    gdf,
    name="Wind farms",
    zoom_on_click=True,
    tooltip=folium.GeoJsonTooltip(fields=["Wind Farm Name","Minimum hub height","Maximum hub height","Cut-in wind speed","Cut-off wind speed","Rated wind speed","Rated power","Manufacturer"]),
    popup=folium.GeoJsonPopup(fields=["Wind Farm Name","Minimum hub height","Maximum hub height","Cut-in wind speed","Cut-off wind speed","Rated wind speed","Rated power","Manufacturer"])
    ).add_to(m)

    # locations = list(zip(gdf['Latitude'], gdf['Longitude']))
    # popups = ["lon:{}<br>lat:{}".format(lon, lat) for (lat, lon) in locations]

    # marker_cluster = MarkerCluster(
    # locations= locations,
    # popups=folium.GeoJsonPopup(fields=["Wind Farm Name","Minimum hub height","Maximum hub height","Cut-in wind speed","Cut-off wind speed","Rated wind speed","Rated power","Manufacturer"]),
    # name="1000 clustered icons",
    # overlay=True,
    # control=True
    # )
    m.save("templates/footprint.html")
    return render_template("footprint.html")

@app.route("/graph")
def graph():
    #Query data from the db.sqlite database
    windfarms = WindfarmWindSpeed.query.all()

    # Convert datetime objects to strings
    formatted_timestamps = [windfarm.timestamp.strftime('%Y-%m-%d %H:%M:%S') for windfarm in windfarms]

    names = [windfarm.name for windfarm in windfarms]
    windspeeds = [windfarm.windspeed for windfarm in windfarms]
    test_type = type(formatted_timestamps[0])
    return render_template("graph.html", timestamps = formatted_timestamps, names = names, windspeeds = windspeeds,test_type = test_type )


@app.route("/data")
def data():
    return render_template("data.html")

@app.route("/database")
def database():
    data = WindfarmWindSpeed.query.all()
    return render_template("sqlitedatabase.html", data = data)

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