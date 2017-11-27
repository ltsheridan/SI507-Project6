# Import statements
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from config import *
import requests
import sys
import json
import csv

# Write code / functions to set up database connection and cursor here.
db_connection, db_cursor = None, None

def get_connection_and_cursor():
    global db_connection, db_cursor
    if not db_connection:
        try:
            if db_password != "":
                db_connection = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
                print("Success connecting to database")
            else:
                db_connection = psycopg2.connect("dbname='{0}' user='{1}'".format(db_name, db_user))
        except:
            print("Unable to connect to the database. Check server and credentials.")
            sys.exit(1) # Stop running program if there's no db connection.

    if not db_cursor:
        db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    return db_connection, db_cursor

db_connection, db_cursor = get_connection_and_cursor()

# Write code / functions to create tables with the columns you want and all database setup here.
def setup_database():
    conn, db_cursor = get_connection_and_cursor()

    db_cursor.execute("DROP TABLE IF EXISTS Sites")
    db_cursor.execute("DROP TABLE IF EXISTS States")


    db_cursor.execute("CREATE TABLE States(ID SERIAL PRIMARY KEY, Name VARCHAR (40) UNIQUE)")
    db_cursor.execute("CREATE TABLE Sites(ID SERIAL PRIMARY KEY, Name VARCHAR(128) UNIQUE, Type VARCHAR(128), State_ID INTEGER REFERENCES States (ID), Location VARCHAR(255), Description TEXT)")
    conn.commit()

# Write code / functions to deal with CSV files and insert data into the database here.

arkansas_data=open('arkansas.csv', 'r')
california_data=open('california.csv','r')
michigan_data=open('michigan.csv','r')
ak_reader=csv.DictReader(arkansas_data)
ca_reader=csv.DictReader(california_data)
mi_reader=csv.DictReader(michigan_data)





# def insert_site(conn, cur, name, type, location, description, state_id):
#     #results=cur.fetchone()[0]
#     sql = "INSERT INTO Sites(Name, Type, Location, Description) VALUES(%s, %s, %s, %s)"
#     cur.execute(sql,(park_name, park_type, park_location, description))
#     conn.commit()


def insert_states(conn, cur, state_name):

    sql = "INSERT INTO States(Name) VALUES(%s)"

    cur.execute(sql,(state_name,))
    id = cur.fetchone()[0]
    conn.commit()
    return id
get_connection_and_cursor()
setup_database()

state_id_mi = insert_states(db_connection, db_cursor, 'michigan')

for site in mi_reader:
    print(site)
    name = site['NAME']
    types = site['TYPE']
    location = site['LOCATION']
    description = site['DESCRIPTION']
    insert_site(db_connection, db_cursor, name, types, location, description, state_id_mi)
    print(site)

# Make sure to commit your database changes with .commit() on the database connection.



# Write code to be invoked here (e.g. invoking any functions you wrote above)

for site in sites_reader:
    print(type(site))
print(results[0])

# Write code to make queries and save data in variables here.






# We have not provided any tests, but you could write your own in this file or another file, if you want.
