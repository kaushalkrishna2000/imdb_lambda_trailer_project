import json
import os
import logging
from trailer_scraper import generate_data
from date_time_extractor import generate_time_data
from pymongo import MongoClient

# Logger initialization
logger = logging.getLogger()
logger.setLevel("INFO")


def lambda_handler(event, context):
    # TODO implement

    # Client creation
    client = MongoClient(os.getenv('MONGO_URI'))

    # Database Connection
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

    resp = collection.find_one_and_replace({
        'date': date_time_metadata['datetime_date'],
        'month': date_time_metadata['datetime_month'],
        'year': date_time_metadata['datetime_year']
    }, imdb_mongo_metadata, upsert=True)
    logger.info(f"Imdb trailer inserted into MongoDB: {resp}")

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda Execution Successfull!')
    }
