import confluent_kafka
import json
import mysql.connector
import os
import sys


def find_current_works():
    with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as db:
        try:
            cursor = mydb.cursor()
            cursor.execute("SELECT location_id FROM current_works")
            results = mycursor.fetchall()
            for row in results:
                location = row[0]
                # TODO: make OpenWheaterAPI



        except mysql.connector.Error as err:
            db.rollback()
            print("Exception raised!\n" + str(err))
            sys.exit("Exiting...")


if __name__ == "__main__":

    # create table current_works if not exists
    with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as mydb:
        try:
            mycursor = mydb.cursor()
            mycursor.execute("CREATE TABLE IF NOT EXISTS currents_works (id INTEGER PRIMARY KEY AUTO_INCREMENT, user_id INTEGER NOT NULL, location_id INTEGER NOT NULL, rules VARCHAR(100000) NOT NULL, time_stamp TIMESTAMP NOT NULL)")
            mydb.commit()  # to make changes effective
        except mysql.connector.Error as err:
            mydb.rollback()
            print("Exception raised!\n" + str(err))
            sys.exit("Exiting...")

    while True:
        find_current_works()