# Import statements
import psycopg2
import psycopg2.extras
from config import *
import csv


# Write code / functions to set up database connection and cursor here.
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

    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    return db_connection, db_cursor

conn, cur = get_connection_and_cursor()

# Write code / functions to create tables with the columns you want and all database setup here.
# Drop both tables FIRST before creating them, because Sites references states
cur.execute('DROP TABLE IF EXISTS "Sites"')
cur.execute('DROP TABLE IF EXISTS "States"')

# Make states table first, so that can refer to it in Sites (Foreign Key)
cur.execute(""" CREATE TABLE IF NOT EXISTS "States"(
    ID SERIAL PRIMARY KEY,
    Name VARCHAR(40) UNIQUE
    )""")
# Now make Sites table, where State_ID references ID in States table (PRIMARY KEY)
cur.execute(""" CREATE TABLE IF NOT EXISTS "Sites"(
    ID SERIAL PRIMARY KEY,
    Name VARCHAR(128) UNIQUE,
    Type VARCHAR(128),
    State_ID INTEGER REFERENCES "States"(ID),
    Location VARCHAR(255),
    Description TEXT
    )""")

# Commit these changes
conn.commit()

# Create States table
cur.execute("""INSERT INTO "States" (Name) VALUES('AR')""")
cur.execute(""" INSERT INTO "States" (Name) VALUES('CA') """)
cur.execute(""" INSERT INTO "States" (Name) VALUES('MI') """)

# Commit these changes
conn.commit()

# Function that cleanly prints query statements
def execute_and_print(query, numer_of_results=1):
    # this function does what lines 27-32 did just now so that you don't have to repeat it every time
    cur.execute(query) # executes a query using the .execute function in psycopg2
    results = cur.fetchall() # AFTER query is made, still need to ascribe a variable to the results
    for r in results[:numer_of_results]: # cut a list only to the 1st index
        print(r) # prints only the first result even tho fetchall is used
    print('--> Result Rows:', len(results)) # prints the length of results using fetchal i.e. the number of columns returned, depending on the query
    print()

# Write code / functions to deal with CSV files and insert data into the database here.

# test - getting a state's ID (integer) through conditional searching using it's name (i.e.'AR')
cur.execute(""" SELECT ID FROM "States" WHERE Name=%s """, ("AR", )) # In SQL, when use %s, it's like a placeholder function, need to pass a tuple, even if just one variable (state_name,)
results = cur.fetchall() # returns a list of dictionaries for the state you queried
print (results)
# [{'id': 1, 'name': 'AR'}]
state_id = results[0]["id"] # Now index and get the ID for that particular state
# 1

# Now put this into a function so can be applied to multiple states and multiple CSV files
def csv_to_db(file_name,state_name):
    fhand = open(file_name,"r") # open your CSV file
    reader = csv.DictReader(fhand) # convert into an iterator, where each row represents a dictionary where key=Column, Value=RowValue

    # Query using the State table to find that state's state id
    cur.execute(""" SELECT ID FROM "States" WHERE Name=%s""", (state_name,)) #
    result = cur.fetchall()
    state_id = result[0]["id"]
    # print(state_id)

    # Iterate through each row in CSV and input data into Sites table
    # Remember, each row, can map keys to values (i.e. row["NAME"]="Isle Royale")
    for row in reader:
        cur.execute(""" INSERT INTO
            "Sites" (Name,Type,State_ID,Location,Description)
            VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING """,
            (row["NAME"],row["TYPE"],state_id,row["LOCATION"],row["DESCRIPTION"]))
    conn.commit()
    print ("Successfully transfered CSV data to Database for",state_name)


# Write code to be invoked here (e.g. invoking any functions you wrote above)
csv_to_db('arkansas.csv','AR')
csv_to_db('california.csv','CA')
csv_to_db('michigan.csv','MI')

# Write code to make queries and save data in variables here.

print('==> Query for all locations of the sites')
cur.execute(""" SELECT Location FROM "Sites" """)
all_locations = cur.fetchall()
# execute_and_print(""" SELECT Location FROM "Sites" """)

print('==> Query for all of the names of the sites whose descriptions incldue the word beautiful')
cur.execute("""SELECT Name FROM "Sites" WHERE Description ILIKE '%beautiful%'""")
beautiful_sites = cur.fetchall()
# execute_and_print("""SELECT Name FROM "Sites" WHERE Description ILIKE '%beautiful%'""")

print('==> Query for total number of sites whose type is national lakeshore')
cur.execute(""" SELECT COUNT(*) FROM "Sites" WHERE Type='National Lakeshore' """)
natl_lakeshores = cur.fetchall()
# execute_and_print(""" SELECT COUNT(*) FROM "Sites" WHERE Type='National Lakeshore' """)

print('==> Query for the names of all the national sites in michigan')
cur.execute(""" SELECT "Sites".Name FROM "Sites" INNER JOIN "States" ON ("Sites".State_ID = "States".ID) WHERE "States".Name='MI' """)
michigan_names = cur.fetchall()
# execute_and_print(""" SELECT "Sites".Name FROM "Sites" INNER JOIN "States" ON ("Sites".State_ID = "States".ID) WHERE "States".Name='MI' """)

print('==> Query for the total number of sites in Arkansas')
cur.execute(""" SELECT COUNT(*) FROM "Sites" INNER JOIN "States" ON ("Sites".State_ID = "States".ID) WHERE "States".Name='AR' """)
total_number_arkansas = cur.fetchall()
# execute_and_print(""" SELECT COUNT(*) FROM "Sites" INNER JOIN "States" ON ("Sites".State_ID = "States".ID) WHERE "States".Name='AR' """)


print ('ALL LOCATIONS', all_locations)
print('BEAUTIFUL SITES', beautiful_sites)
print('NATIONAL LAKESHORES', natl_lakeshores)
print('MICHIGAN NAMES', michigan_names)
print('TOTAL NUMBER ARKANSAS',total_number_arkansas)

# We have not provided any tests, but you could write your own in this file or another file, if you want.
