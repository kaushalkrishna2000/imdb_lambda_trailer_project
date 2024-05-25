import os
import logging
from pymongo import MongoClient
from trailer_scraper import generate_data
from date_time_extractor import generate_time_data

#Logger initialization
logger = logging.getLogger()

#Client creation
client=MongoClient(os.getenv('MONGO_URI'))
# client = MongoClient("mongodb+srv://kaushalkrishna2000:K24052005k@imdbserverlesssample.c0cybkx.mongodb.net")

#Database Connection
database = client.get_database("imdb_trailer")
collection = database.get_collection("daily")

imdb_trailer_metadata = generate_data()
date_time_metadata = generate_time_data()

logger.info(f"Imdb trailer metadata: {imdb_trailer_metadata}")
logger.info(f"Date time metadata: {date_time_metadata}")

imdb_mongo_metadata = {
    'timestamp': date_time_metadata['timestamp'],
    'day_of_week': date_time_metadata['day_of_week'],
    'day_of_week_number': date_time_metadata['day_of_week_number'],
    'week_number': date_time_metadata['week_number'],
    'date': date_time_metadata['datetime_date'],
    'month': date_time_metadata['datetime_month'],
    'year': date_time_metadata['datetime_year'],
    'details': imdb_trailer_metadata
}

logger.info(f"Imdb trailer mongo metadata: {imdb_mongo_metadata}")

resp=collection.insert_one(imdb_trailer_metadata)

logger.info(f"Imdb trailer inserted into MongoDB: {resp}")