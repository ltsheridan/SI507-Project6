# Import statements
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from config import *
import requests
import sys
import json
from csv import DictReader

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

#db_connection, db_cursor = get_connection_and_cursor()

# Write code / functions to create tables with the columns you want and all database setup here.
def setup_database():
    conn, db_cursor = get_connection_and_cursor()

    db_cursor.execute("DROP TABLE IF EXISTS Sites")
    db_cursor.execute("DROP TABLE IF EXISTS States")


    db_cursor.execute("CREATE TABLE States(ID SERIAL PRIMARY KEY, Name VARCHAR (40) UNIQUE)")
    db_cursor.execute("CREATE TABLE Sites(ID SERIAL PRIMARY KEY, Name VARCHAR(128) UNIQUE, Type VARCHAR(128), State_ID INTEGER REFERENCES States (ID), Location VARCHAR(255), Description TEXT)")
    conn.commit()

# Write code / functions to deal with CSV files and insert data into the database here.

# arkansas_data=open('arkansas.csv', 'r')
# california_data=open('california.csv','r')
# michigan_data=open('michigan.csv','r')
# ak_reader=csv.DictReader(arkansas_data)
# ca_reader=csv.DictReader(california_data)
# mi_reader=csv.DictReader(michigan_data)



def insert_site(name, type, location, description, state_id):
    sql = "INSERT INTO Sites(Name, Type, Location, Description, state_id) VALUES(%s, %s, %s, %s, %s)"
    db_cursor.execute(sql,(name, type, location, description, state_id))
    db_connection.commit()

#
def insert_states(state_name, csv):
    sql = "INSERT INTO States(Name) VALUES(%s) RETURNING id"
    db_cursor.execute(sql,(state_name,))
    state_id = db_cursor.fetchone()['id']
    state_data = open(csv,'r')
    reader = DictReader(state_data)

    for site in reader:
        name = site['NAME']
        types = site['TYPE']
        location = site['LOCATION']
        description = site['DESCRIPTION']
        insert_site(name, types, location, description, state_id)

    db_connection.commit()
    return id




# Make sure to commit your database changes with .commit() on the database connection.



# Write code to be invoked here (e.g. invoking any functions you wrote above)
get_connection_and_cursor()
setup_database()
insert_states('michigan','michigan.csv')
insert_states('arkansas','arkansas.csv')
insert_states('california','california.csv')


# Write code to make queries and save data in variables here.

def execute(query, numer_of_results=1):
    db_cursor.execute(query)
    results = db_cursor.fetchall()


all_locations=execute("select location from Sites")
beautiful_sites=execute("""select Name from Sites where Description ilike '%beautiful%'""")
natl_lakeshores=execute("select count(Type) from Sites where Type='National Lakeshore'")
michigan_names=execute("select Sites.Name from Sites INNER JOIN States ON (States.ID=Sites.ID)")
total_number_arkansas=("select count(Sites.ID) from Sites INNER JOIN States ON (States.ID=Sites.ID) where Name ilike '%Arkansas%'")


# We have not provided any tests, but you could write your own in this file or another file, if you want.
