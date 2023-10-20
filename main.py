from flask import Flask
from flask import render_template
from folium.plugins import MarkerCluster
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
from geoalchemy2 import Geometry
import csv
import folium
import geopandas as gpd
import datetime
import requests
import xml.etree.ElementTree as ET
from flask_apscheduler import APScheduler

# set configuration values
class Config:
    SCHEDULER_API_ENABLED = True

# create app
app = Flask(__name__)
app.debug = True
app.config.from_object(Config())

# initialize scheduler
scheduler = APScheduler()
# if you don't wanna use a config, you can set options here:
# scheduler.api_enabled = True
scheduler.init_app(app)
scheduler.start()

# interval example
@scheduler.task('interval', id='do_job_1', seconds=300, misfire_grace_time=900)
def job1():
    print('Job 1 executed')

@app.route("/")
def index():
    return render_template("index.html")

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
    m.save("venv/templates/footprint.html")
    return render_template("footprint.html")


@app.route("/data")
def data():
    return render_template("data.html")

@app.route("/windspeed")
def windspeed():
    df = pd.read_csv("Windfarm_WebScraped_DataV3.csv")
    current_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M')
    hour_ago_time = datetime.datetime.now() - datetime.timedelta(hours = -1)
    hour_ago_time = hour_ago_time.strftime('%Y-%m-%dT%H:%M')
    url = 'http://metwdb-openaccess.ichec.ie/metno-wdb2ts/locationforecast'

    windspeeds= []

    for index,row in df.iterrows():
        lat = str(row['Latitude'])
        long = str(row['Longitude'])
        qfrom = current_time
        qto = hour_ago_time
        query= {"lat": lat, "long": long, "from":qfrom, "to": qto}
        response = requests.get(url, params = query)
        root = ET.fromstring(response.content)
        x = [time.attrib['from'] for time in root.iter('time')]
        x = [item for item in x[::2]]
        x = [datetime.datetime.strptime(time,"%Y-%m-%dT%H:%M:%SZ") for time in x]
        y = [windSpeed.attrib['mps'] for windSpeed in root.iter('windSpeed')]
        y = [float(speed) for speed in y]
        windspeeds.append(y)
    return render_template("windspeed.html", windspeeds = windspeeds)
