# Import statements
import psycopg2
import psycopg2.extras
import csv
from config import *

# Write code / functions to set up database connection and cursor here.
DEBUG = False

def get_connection_and_cursor():
    try:
        if db_password != "":
            db_connection = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
            print("Success connecting to database")
        else:
            db_connection = psycopg2.connect("dbname='{0}' user='{1}'".format(db_name, db_user))
    except:
        print("Unable to connect to the database. Check server and credentials.")
        sys.exit(1) # Stop running program if there's no db connection.

    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    return db_connection, db_cursor


conn, cur = get_connection_and_cursor()


# Write code / functions to create tables with the columns you want and all database setup here.

# Table States
if DEBUG:
    print("Create TABLE States")
cur.execute("DROP TABLE IF EXISTS States CASCADE")
cur.execute("""CREATE TABLE IF NOT EXISTS States(
                id SERIAL PRIMARY KEY,
                name VARCHAR(40) UNIQUE
                )"""
            )

# Table Sites
if DEBUG:
    print("Create TABLE Sites")

cur.execute("DROP TABLE IF EXISTS Sites")
cur.execute("""CREATE TABLE IF NOT EXISTS Sites(
                id SERIAL PRIMARY KEY,
                name VARCHAR(128) UNIQUE,
                type VARCHAR(128),
                state_id INTEGER REFERENCES states(id),
                location VARCHAR(255),
                description TEXT
                )"""
            )

conn.commit() # Necessary to save changes in database

if DEBUG:
    print("Table created successfully.")


# Write code / functions to deal with CSV files and insert data into the database here.
def read_csv(file_name):
    dict_list = []
    with open(file_name,'r') as f:
        reader = csv.DictReader(f)  # ref: https://overlaid.net/2016/02/04/convert-a-csv-to-a-dictionary-in-python/
        for row in reader:
            dict_list.append(row)
    # print(dict_list[:3])
    return dict_list


def insert_states(state_list):
    for state in state_list:
        cur.execute("""INSERT INTO States(name) VALUES (%s) RETURNING id""", (state,))
        id = cur.fetchone()['id']  # ref: https://stackoverflow.com/questions/1438430/pygresql-insert-and-return-serial
        if DEBUG:
            print("insert {0} | {1}".format(id, state))
    conn.commit()



def insert_sites(dict_list, state_id):
    for row in dict_list:
        row['STATE_ID'] = state_id
        cur.execute("""INSERT INTO
                    Sites(name, type, state_id, location, description)
                    VALUES(%(NAME)s, %(TYPE)s, %(STATE_ID)s, %(ADDRESS)s, %(DESCRIPTION)s)
                    RETURNING id""",
                    row)
        id = cur.fetchone()['id']
        if DEBUG:
            print("insert id: {0} | name: {1} | type: {2} | location: {3} | state id: {4}".format(
            id, row['NAME'], row['TYPE'], row['ADDRESS'], row['STATE_ID'])
            )
    conn.commit()



# Make sure to commit your database changes with .commit() on the database connection.
# Create state list and insert into TABLE States
state_list = ['arkansas', 'california', 'michigan']
insert_states(state_list)

# Insert arkansas.csv into TABLE Sites
cur.execute("SELECT * FROM States WHERE name = 'arkansas'")
arkansas_state_id = cur.fetchone()['id']
arkansas_dict_list = read_csv('arkansas.csv')
insert_sites(arkansas_dict_list, arkansas_state_id)

# Insert california.csv into TABLE Sites
cur.execute("SELECT * FROM States WHERE name = 'california'")
california_state_id = cur.fetchone()['id']
california_dict_list = read_csv('california.csv')
insert_sites(california_dict_list, california_state_id)

# Insert michigan.csv into TABLE Sites
cur.execute("SELECT * FROM States WHERE name = 'michigan'")
michigan_state_id = cur.fetchone()['id']
michigan_dict_list = read_csv('michigan.csv')
insert_sites(michigan_dict_list, michigan_state_id)



# Write code to be invoked here (e.g. invoking any functions you wrote above)

def execute_and_print(query, numer_of_results=1):
    cur.execute(query)
    results = cur.fetchall()
    for r in results[:numer_of_results]:
        print(r)
    print('--> Result Rows:', len(results))
    print()
    return results


# Write code to make queries and save data in variables here.
print("==> Get locations of all sites")
all_locations = execute_and_print("SELECT location from Sites", 5)
# print(all_locations[:5])

print("==> Get names of all sites whose description includes beautiful")
beautiful_sites = execute_and_print("SELECT name from Sites WHERE description LIKE '%beautiful%'", 5)
# print(beautiful_sites)

print("==> Get counts of all sites whose type is National Lakeshore")
natl_lakeshores = execute_and_print("SELECT count(*) from Sites WHERE type = 'National Lakeshore'", 5)
# print(natl_lakeshores)

print("==> Get names of all national sites in Michigan")
michigan_names = execute_and_print("SELECT Sites.name from Sites INNER JOIN States ON Sites.state_id = States.id AND States.name = 'michigan'", 10)
# print(michigan_names)

print("==> Get counts of sites in Arkansas")
total_number_arkansas = execute_and_print("SELECT count(Sites.name) from Sites INNER JOIN States ON Sites.state_id = States.id AND States.name = 'arkansas'", 1)
print(total_number_arkansas)





# We have not provided any tests, but you could write your own in this file or another file, if you want.
