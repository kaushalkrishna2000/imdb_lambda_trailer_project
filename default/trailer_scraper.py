import datetime
import pytz
import logging

import requests
from bs4 import BeautifulSoup

#Logger initialization
logger = logging.getLogger()


# Title Extractor
def title_extractor(element):
    return element.find('a', {'class': 'ipc-poster-card__title'}).text


# Generate Metadata
def generate_metadata(soup, record_time):
    imdb_trending_trailer_meta = []
    for ele in soup.find_all('div', {'class': 'ipc-poster-card'}):
        title = title_extractor(ele)
        imdb_trending_trailer_meta.append({
            'name': title,
            'record_time': record_time,
            'count': 1
        })
    return imdb_trending_trailer_meta


# Actual Calling Function
def generate_data():
    # Configuring base site
    base_site = "https://www.imdb.com/trailers/?ref_=hm_hp_sm"

    # Webpage header
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/115.0.0.0 Safari/537.36 OPR/102.0.0.0'
    }

    # Initialize request
    r = requests.get(base_site, headers=headers)
    logger.info(r, r.status_code, r.reason)

    # Parsing the webpage
    soup = BeautifulSoup(r.content, "lxml")

    # Define IST time zone
    ist = pytz.timezone('Asia/Kolkata')

    # Record Date Time
    record_time = str(datetime.datetime.now(tz=ist))
    logger.info(record_time)

    # Generate Metadata
    imdb_meta_list = generate_metadata(soup, record_time)

    return imdb_meta_list
