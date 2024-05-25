import json
import logging
import os

from pymongo import MongoClient

from date_time_extractor import generate_time_data

# Logger initialization
logger = logging.getLogger()
logger.setLevel("INFO")


def lambda_handler(event, context):
    # TODO implement

    # Client creation
    client = MongoClient(os.getenv('MONGO_URI'))

    # Database Connection
    database = client.get_database("imdb_trailer")
    collection_weekly = database.get_collection("weekly")
    collection_monthly = database.get_collection("monthly")

    date_time_metadata = generate_time_data()
    logger.info(f"Date time metadata: {date_time_metadata}")

    month = date_time_metadata['datetime_month']

    prev_month_data = collection_weekly.find({'month': month})

    monthly_dict = {}

    # Performing Aggregation
    for data_point in prev_month_data:
        logger.info(data_point)
        details = data_point['details']
        for element in details:
            if element in monthly_dict:
                monthly_dict[element] += details[element]
            else:
                monthly_dict[element] = details[element]

    # Creating the payload
    imdb_mongo_metadata = {
        'record_time': date_time_metadata['timestamp'],
        'month': date_time_metadata['datetime_month'],
        'year': date_time_metadata['datetime_year'],
        'details': monthly_dict
    }

    logger.info(f"Imdb trailer mongo metadata: {imdb_mongo_metadata}")
    print(imdb_mongo_metadata)

    # Insertion
    resp = collection_monthly.find_one_and_replace({'month': month}, imdb_mongo_metadata, upsert=True)
    logger.info(f"Imdb trailer inserted into MongoDB: {resp}")

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda Execution Successfull!')
    }
