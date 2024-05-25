import requests


def dayOfWeekExtractor(day_of_week: int):
    day_mapping = dict(zip(range(0, 7), ["Sunday","Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]))
    return day_mapping[day_of_week]


def datetime_parser(date_string: str, mode: str = "date"):
    date_string_parsed = date_string.split("T")[0]

    if mode == "date":
        return date_string_parsed.split("-")[2]

    if mode == "month":
        return date_string_parsed.split("-")[1]

    if mode == "year":
        return date_string_parsed.split("-")[0]


def generate_time_data():
    response = requests.get("https://worldtimeapi.org/api/timezone/Asia/Kolkata")

    date_response = response.json()

    date_dict = {
        'timestamp': date_response['datetime'],
        'day_of_week_number': date_response['day_of_week'],
        'day_of_week': dayOfWeekExtractor(date_response['day_of_week']),
        'week_number': date_response['week_number'],
        'datetime_date': datetime_parser(date_response['datetime'], "date"),
        'datetime_month': datetime_parser(date_response['datetime'], "month"),
        'datetime_year': datetime_parser(date_response['datetime'], "year"),
    }

    return date_dict