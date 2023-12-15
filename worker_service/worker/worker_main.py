import confluent_kafka
import json
import mysql.connector
import os
import sys
import requests


def make_query(query):
    try:
        resp = requests.get(url=query)
        resp.raise_for_status()
        data = resp.json()
        if data['cod'] != 200:
            raise Exception('Query failed: ' + data.get('message'))
        print(data)
        return data
    except requests.exceptions.RequestException as e:
        print(f'Request failed: {e}')
    except Exception as e:
        print(f'Error: {e}')

def check_rules(db_cursor,api_response):
    db_cursor.execute("SELECT rules FROM current_works")
    rules_list = db_cursor.fetchall()
    event_dict = dict()
    for rules in rules_list:
        user_violated_rules_list = list()
        rules_json = json.loads(rules[0])
        keys_set_target = set(rules_json.keys())
        for key in keys_set_target:
            if "max" in key:
                if api_response.get(key) > rules_json.get(key):
                    temp_dict = dict()
                    temp_dict[key] = api_response.get(key)
                    user_violated_rules_list.append(temp_dict)
                    temp_dict.clear()
                    event_dict[rules.get("user_id")] = user_violated_rules_list
            if "min" in key:
                if api_response.get(key) < rules_json.get(key):
                    temp_dict = dict()
                    temp_dict[key] = api_response.get(key)
                    user_violated_rules_list.append(temp_dict)
                    temp_dict.clear()
                    event_dict[rules.get("user_id")] = user_violated_rules_list
            # if "rain" and "snow" are in keys_set_target then these rules are of interest to the user
            # so rules_json.get("rain"/"snow") is surely True
            if key == "rain" and api_response.get("rain") == True:
                temp_dict = dict()
                temp_dict[key] = api_response.get(key)
                user_violated_rules_list.append(temp_dict)
                temp_dict.clear()
                event_dict[rules.get("user_id")] = user_violated_rules_list
            if key == "snow" and api_response.get("snow") == True:
                temp_dict = dict()
                temp_dict[key] = api_response.get(key)
                user_violated_rules_list.append(temp_dict)
                temp_dict.clear()
                event_dict[rules.get("user_id")] = user_violated_rules_list
            if key == "wind_direction":
                if rules_json.get(key) == api_response.get(key):
                    temp_dict = dict()
                    temp_dict[key] = api_response.get(key)
                    user_violated_rules_list.append(temp_dict)
                    temp_dict.clear()
                    event_dict[rules.get("user_id")] = user_violated_rules_list



# function to formatting data returned by OpenWheater API according to our business logic
def format_data(data):
    output_json_dict = dict()
    output_json_dict['max_temp'] = data["main"]["temp"]
    output_json_dict['min_temp'] = data["main"]["temp"]
    output_json_dict['max_humidity'] = data["main"]["humidity"]
    output_json_dict['min_humidity'] = data["main"]["humidity"]
    output_json_dict['max_pressure'] = data["main"]["pressure"]
    output_json_dict['min_pressure'] = data["main"]["pressure"]
    output_json_dict["max_wind_speed"] = data["wind"]["speed"]
    output_json_dict["min_wind_speed"] = data["wind"]["speed"]
    output_json_dict["clouds_max"] = data["clouds"]["all"]
    output_json_dict["clouds_min"] = data["clouds"]["all"]
    direction_list = ["N", "NE", "E", " SE", "S", "SO", "O", "NO"]
    j = 0
    for i in range(0, 315, 45):
        if (i - 22.5) < data["wind"]["deg"] < (i + 22.5):
            output_json_dict["wind_direction"] = direction_list[j]
            j=j+1
    if data["weather"]["main"] == "Rain":
        output_json_dict["rain"] = True
    else:
        output_json_dict["rain"] = False
    if data["weather"]["main"] == "Snow":
        output_json_dict["snow"] = True
    else:
        output_json_dict["snow"] = False
    return output_json_dict


# function for recovering unchecked rules when worker goes down before publishing notification event
def find_current_works():
    with (mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as db):
        try:
            cursor = db.cursor()
            cursor.execute("SELECT location_id FROM current_works GROUP BY location_id")
            results = cursor.fetchall()
            for row in results:
                location_id = row[0]
                # make OpenWheater API call
                apikey = os.environ.get('APIKEY')
                cursor.execute("SELECT * FROM locations WHERE id=%s", (location_id,))
                location_row_list = cursor.fetchall()
                location_row = location_row_list[0]  # the only row returned
                rest_call = f"https://api.openweathermap.org/data/2.5/weather?lat={location_row[2]}&lon={location_row[3]}&appid={apikey}"
                data = make_query(rest_call)
                formatted_data = format_data(data)
                check_rules(cursor, formatted_data)





        except mysql.connector.Error as err:
            db.rollback()
            print("Exception raised!\n" + str(err))
            sys.exit("Exiting...")


if __name__ == "__main__":

    # create table locations if not exists
    with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as db:
        try:
            cursor = db.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS locations (id INTEGER PRIMARY KEY AUTO_INCREMENT, name VARCHAR(40) NOT NULL, lat FLOAT NOT NULL, long FLOAT NOT NULL, state_code varchar(30), country_code VARCHAR(10))")
            db.commit()  # to make changes effective
        except mysql.connector.Error as err:
            db.rollback()
            print("Exception raised!\n" + str(err))
            sys.exit("Exiting...")

    # create table current_works if not exists
    with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as mydb:
        try:
            mycursor = mydb.cursor()
            mycursor.execute("CREATE TABLE IF NOT EXISTS currents_works (id INTEGER PRIMARY KEY AUTO_INCREMENT, user_id INTEGER NOT NULL, location_id INTEGER NOT NULL, rules VARCHAR(100000) NOT NULL, time_stamp TIMESTAMP NOT NULL, FOREIGN KEY(location_id) REFERENCES locations(id))")
            mydb.commit()  # to make changes effective
        except mysql.connector.Error as err:
            mydb.rollback()
            print("Exception raised!\n" + str(err))
            sys.exit("Exiting...")

    while True:
        find_current_works()