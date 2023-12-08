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


async def gather_wind_data():
    windfarm_df = pd.read_csv("Windfarm_WebScraped_DataV3.csv")
    coroutine_objects = []
    now = datetime.now().replace(microsecond=0, second=0, minute=0)
    current_time = now.strftime('%Y-%m-%dT%H:%M')
    database_timestamp = datetime.strptime(current_time,"%Y-%m-%dT%H:%M")
    async with aiohttp.ClientSession() as session:  
        for index, row in windfarm_df.iterrows():
            lon = row['Longitude']
            lat = row['Latitude']
            name = row['Wind Farm Name']
            cutin_speed = row['Cut-in wind speed']
            cutoff_speed = row['Cut-off wind speed']
            rotor_radius = row['Rotor diameter']
            number_of_turbines = row['Number of Turbines']
            async with asyncio.TaskGroup() as task_group:
                reponse_texts = task_group.create_task(gather_session_urls(session,name,lon,lat,current_time))
        print(reponse_texts)
        
# root = ET.fromstring(response_text)
#             wind_speed_element = root.find(".//windSpeed")


async def gather_session_urls(session,name,lon,lat,current_time):
    url = 'http://metwdb-openaccess.ichec.ie/metno-wdb2ts/locationforecast'
    qto = current_time
    qfrom = qto
    params= {"lat": lat, "long": lon, "from":qfrom, "to": qto}    
    async with session.get(url, params=params) as resp:
        try:
            print(resp.text())
            return resp.text()
        except Exception as e:
            print(f"Error processing wind data for lat={lat}, lon={lon}: {e}")
            return None

if __name__ == '__main__':
    asyncio.run(gather_wind_data())
    # app.run(debug=True)