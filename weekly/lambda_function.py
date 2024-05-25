import json
import logging
import os

from pymongo import MongoClient

from date_time_extractor import generate_time_data

# Logger initialization
logger = logging.getLogger()
logger.setLevel("INFO")


def lambda_handler():
    # TODO implement

    # Client creation
    client = MongoClient(os.getenv('MONGO_URI'))

    # Database Connection
    database = client.get_database("imdb_trailer")
    collection_daily = database.get_collection("daily")
    collection_weekly = database.get_collection("weekly")

    date_time_metadata = generate_time_data()
    logger.info(f"Date time metadata: {date_time_metadata}")

    week_number = date_time_metadata['week_number']

    prev_week_data = collection_daily.find({'week_number': week_number})
    print(prev_week_data)

    weekly_dict = {}
    weekly_date_range = []

    # Performing Aggregation
    for data_point in prev_week_data:
        print(data_point)
        weekly_date_range.append(data_point['date'])
        details = data_point['details']
        for element in details:
            if element in weekly_dict:
                weekly_dict[element] += 1
            else:
                weekly_dict[element] = 1

    # Creating the payload
    imdb_mongo_metadata = {
        'record_time': date_time_metadata['timestamp'],
        'week_number': week_number,
        'week_range': weekly_date_range,
        'month': date_time_metadata['datetime_month'],
        'year': date_time_metadata['datetime_year'],
        'details': weekly_dict
    }

    logger.info(f"Imdb trailer mongo metadata: {imdb_mongo_metadata}")
    print(imdb_mongo_metadata)
    # Insertion
    resp = collection_weekly.find_one_and_replace({'week_number': week_number}, imdb_mongo_metadata, upsert=True)
    logger.info(f"Imdb trailer inserted into MongoDB: {resp}")

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda Execution Successfull!')
    }