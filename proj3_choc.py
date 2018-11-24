import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

conn = sqlite3.connect(DBNAME)
cur = conn.cursor()

statement = '''
    DROP TABLE IF EXISTS 'Bars';
'''
cur.execute(statement)

statement = '''
    DROP TABLE IF EXISTS 'Countries';
'''
cur.execute(statement)

#Creating the table "Bars"
statement = '''
    CREATE TABLE 'Bars' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Company' TEXT NOT NULL,
        'SpecificBeanBarName' TEXT NOT NULL,
        'REF' TEXT NOT NULL,
        'ReviewDate' TEXT NOT NULL,
        'CocoaPercent' REAL NOT NULL,
        'CompanyLocationId' INTEGER,
        'Rating' REAL NOT NULL,
        'BeanType' TEXT,
        'BroadBeanOriginId' INTEGER
    );
'''
cur.execute(statement)
conn.commit()

#Creating the table "Countries"
statement = '''
    CREATE TABLE 'Countries' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Alpha2' TEXT NOT NULL,
            'Alpha3' TEXT NOT NULL,
            'EnglishName' TEXT NOT NULL,
            'Region' TEXT NOT NULL,
            'Subregion' TEXT NOT NULL,
            'Population' INTEGER NOT NULL,
            'Area' REAL
    );
'''
cur.execute(statement)
conn.commit()

with open('countries.json') as f:
    data_list = json.load(f)
    
    for country in data_list:
        country_name = (country['name'])
        alpha2 = (country['alpha2Code'])
        alpha3 = (country['alpha3Code'])
        country_reg = (country['region'])
        country_subreg = (country['subregion'])
        country_pop = (country['population'])
        country_area = (country['area'])
        statement = "INSERT INTO \"Countries\" (Alpha2, Alpha3, EnglishName, Region, Subregion, Population, Area) VALUES (?, ?, ?, ?, ?, ?, ?)"
        cur.execute(statement, (alpha2, alpha3, country_name, country_reg, country_subreg, country_pop, country_area))
    f.close()
conn.commit()

with open('flavors_of_cacao_cleaned.csv') as f:
    csvReader = csv.reader(f)
    next(csvReader)
    for row in csvReader:
        statement = "SELECT Id FROM Countries WHERE EnglishName = ?"
        company_location = row[5]
        cur.execute(statement, (company_location,))
        company_location_id = cur.fetchone()
        statement = "SELECT Id FROM Countries WHERE EnglishName = ?"
        cur.execute(statement, (row[8],))
        broadbean_origin_id = cur.fetchone()
        
        if company_location_id is not None and broadbean_origin_id is not None:
            statement = "INSERT INTO \"Bars\" (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, Rating, BeanType, CompanyLocationId, BroadBeanOriginId) VALUES (?, ?, ?, ?, ?, ?, ?,?,?)"
            cur.execute(statement, (row[0], row[1], row[2], row[3], row[4], row[6], row[7],company_location_id[0],broadbean_origin_id[0]))
        elif company_location_id is None and broadbean_origin_id is not None:
            statement = "INSERT INTO \"Bars\" (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, Rating, BeanType, BroadBeanOriginId) VALUES (?, ?, ?, ?, ?, ?,?,?)"
            cur.execute(statement, (row[0], row[1], row[2], row[3], row[4], row[6], row[7], broadbean_origin_id[0]))
        else:
            statement = "INSERT INTO \"Bars\" (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, Rating, BeanType, CompanyLocationId) VALUES (?, ?, ?, ?, ?, ?,?,?)"
            cur.execute(statement, (row[0], row[1], row[2], row[3], row[4], row[6], row[7], company_location_id[0]))
    
    f.close()
conn.commit()

conn.close()


## Part 2: Implement logic to process user commands
#def process_command(command):
#    return []
#
#
#def load_help_text():
#    with open('help.txt') as f:
#        return f.read()
#
## Part 3: Implement interactive prompt. We've started for you!
#def interactive_prompt():
#    help_text = load_help_text()
#    response = ''
#    while response != 'exit':
#        response = input('Enter a command: ')
#
#        if response == 'help':
#            print(help_text)
#            continue
#
## Make sure nothing runs or prints out when this file is run as a module
#if __name__=="__main__":
#    interactive_prompt()