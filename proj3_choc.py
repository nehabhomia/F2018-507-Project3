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

# populating the table countries
with open(COUNTRIESJSON) as f:
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

#populating the table bars
with open(BARSCSV) as f:
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
# to populate the columns CompanyLocationId and BroadBeanOriginId from countries
#table using EnglishName and to be able to handle there being Null values in either        
        if company_location_id is not None and broadbean_origin_id is not None:
            statement = "INSERT INTO \"Bars\" (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, Rating, BeanType, CompanyLocationId, BroadBeanOriginId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            cur.execute(statement, (row[0], row[1], row[2], row[3], row[4].replace('%', ''), row[6], row[7],company_location_id[0],broadbean_origin_id[0]))
        elif company_location_id is None and broadbean_origin_id is not None:
            statement = "INSERT INTO \"Bars\" (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, Rating, BeanType, BroadBeanOriginId) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            cur.execute(statement, (row[0], row[1], row[2], row[3], row[4].replace('%', ''), row[6], row[7], broadbean_origin_id[0]))
        elif company_location_id is not None and broadbean_origin_id is None:
            statement = "INSERT INTO \"Bars\" (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, Rating, BeanType, CompanyLocationId) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            cur.execute(statement, (row[0], row[1], row[2], row[3], row[4].replace('%', ''), row[6], row[7], company_location_id[0]))
        else:
            statement = "INSERT INTO \"Bars\" (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, Rating, BeanType) VALUES (?, ?, ?, ?, ?, ?, ?)"
            cur.execute(statement, (row[0], row[1], row[2], row[3], row[4].replace('%', ''), row[6], row[7]))
    f.close()
conn.commit()

conn.close()


# Part 2: Implement logic to process user commands
def process_command(command):
    '''
    This function will split the user input and based on whether the command is
    bars, companies, countries or regions, it will run the appropriate function
    to process the parameters and return a list of the appropriate result.
    '''
    command_parameters = command.split(" ")
    split_command = command_parameters[0]
    parameters = command_parameters[1:]
    if split_command == "bars":
        result_list = processBars(parameters)
    elif split_command == "companies":
        result_list = processCompanies(parameters)
    elif split_command == "countries":
        result_list = processCountries(parameters)
    elif split_command == "regions":
        result_list = processRegions(parameters)
    else:
        print("Command not recognized: ", command)
    return result_list #if it goes into else block, there is no result_list, so will that cause an error?


def processBars(parameters):
    '''
    This function will run if the user command is bars, and will process the
    parameters according to the given list to give desired result.
    '''
    possible_where = ["sellcountry", "sourcecountry", "sellregion", "sourceregion"]
    possible_limit_by = ["top", "bottom"]
    where_clause_elements = []
    sort_by_elements = []
    limit_elements = []
    for param in parameters:
        if "=" in param and param.split("=")[0] in possible_where:
            key = param.split("=")[0]
            value = param.split("=")[1]
            if key == "sellcountry":
                where_clause_elements.append("C1.Alpha2=" + "'" + value + "'")
            if key == "sourcecountry":
                where_clause_elements.append("C2.Alpha2=" + "'" + value + "'")
            if key == "sellregion":
                where_clause_elements.append("C1.Region=" + "'" + value + "'")
            if key == "sourceregion":
                where_clause_elements.append("C2.Region=" + "'" + value + "'")
        elif "=" in param and param.split("=")[0] in possible_limit_by:
            key = param.split("=")[0]
            value = param.split("=")[1]
            limit_elements.append([key, int(value)])
        elif param == "ratings":
            sort_by_elements.append("Rating")
        elif param == "cocoa":
            sort_by_elements.append("CocoaPercent")
        else:
            print("Command not recognized")
            return
    if not sort_by_elements:
        sort_by_elements.append("Rating")
    if not limit_elements:
        limit_elements.append(["top", 10])
    if not where_clause_elements:
        where_clause = ""
    else:
        where_clause = "WHERE " + 'and'.join(where_clause_elements)

    sort_by_clause_top = "ORDER BY " + sort_by_elements[0] + " DESC"
    sort_by_clause_bottom = "ORDER BY " + sort_by_elements[0]

    statement = """SELECT SpecificBeanBarName, Company, C1.EnglishName, Rating, CocoaPercent, C2.EnglishName
                FROM Bars
                LEFT JOIN Countries C1 ON C1.Id = Bars.CompanyLocationId
                LEFT JOIN Countries C2 ON C2.Id = Bars.BroadBeanOriginId
                %s
                %s
                """
    conn = sqlite3.connect('choc.db')
    cur = conn.cursor()
    limit_elements_clause = limit_elements[0]
    if 'bottom' in limit_elements_clause:
        cur.execute(statement % (where_clause, sort_by_clause_bottom))
    else:
        cur.execute(statement % (where_clause, sort_by_clause_top))
    table = cur.fetchall()
    result = table[:limit_elements_clause[1]]
    return result


def processCompanies(parameters):
    '''
    This function will run if the user command is companies, and will process the
    parameters according to the given list to give desired result.
    '''
    possible_where = ["country", "region"]
    possible_limit_by = ["top", "bottom"]
    where_clause_elements = []
    sort_by_elements = []
    sort_by_select = []
    limit_elements = []
    for param in parameters:
        if "=" in param and param.split("=")[0] in possible_where:
            key = param.split("=")[0]
            value = param.split("=")[1]
            if key == "country":
                where_clause_elements.append("C1.Alpha2=" + "'" + value + "'")
            if key == "region":
                where_clause_elements.append("C1.Region=" + "'" + value + "'")
        elif "=" in param and param.split("=")[0] in possible_limit_by:
            key = param.split("=")[0]
            value = param.split("=")[1]
            limit_elements.append([key, int(value)])
        elif param == "ratings":
            sort_by_select = "AVG(Rating) as Rating"
            sort_by_elements.append("Rating")
        elif param == "cocoa":
            sort_by_select = "AVG(CocoaPercent) as CocoaPercent"
            sort_by_elements.append("CocoaPercent")
        elif param == "bars_sold":
            sort_by_select = "Count(*) as bars_sold"
            sort_by_elements.append("bars_sold")
        else:
            print("Command not recognized")
            return

    if not sort_by_elements:
        sort_by_select = "AVG(Rating) as Rating"
        sort_by_elements.append("Rating")
    if not limit_elements:
        limit_elements.append(["top", 10])
    if not where_clause_elements:
        where_clause = ""
    else:
        where_clause = "WHERE " + 'and'.join(where_clause_elements)
    sort_by_clause_top = "ORDER BY " + sort_by_elements[0] + " DESC"
    sort_by_clause_bottom = "ORDER BY " + sort_by_elements[0]
    group_by_clause = "GROUP BY Company"
    statement = """SELECT Company, C1.EnglishName, %s
                    FROM Bars
                        JOIN Countries C1 ON C1.Id = Bars.CompanyLocationId
                        %s
                        %s
                        HAVING count(*) > 4
                        %s
                        """
    conn = sqlite3.connect('choc.db')
    cur = conn.cursor()
    limit_elements_clause = limit_elements[0]
    if 'bottom' in limit_elements_clause:
        cur.execute(statement % (sort_by_select,where_clause, group_by_clause, sort_by_clause_bottom))
    else:
        cur.execute(statement % (sort_by_select,where_clause, group_by_clause, sort_by_clause_top))
    table = cur.fetchall()
    result = table[:limit_elements_clause[1]]
    return result


def processCountries(parameters):
    '''
    This function will run if the user command is countries, and will process the
    parameters according to the given list to give desired result.
    '''
    possible_where = ["region"]
    possible_limit_by = ["top", "bottom"]
    where_clause_elements = []
    sort_by_elements = []
    sort_by_select = []
    limit_elements = []
    join_on = "Bars.CompanyLocationId"
    for param in parameters:
        if "=" in param and param.split("=")[0] in possible_where:
            key = param.split("=")[0]
            value = param.split("=")[1]
            if key == "region":
                where_clause_elements.append("C1.Region=" + "'" + value + "'")
        elif "=" in param and param.split("=")[0] in possible_limit_by:
            key = param.split("=")[0]
            value = param.split("=")[1]
            limit_elements.append([key, int(value)])
        elif param == "ratings":
            sort_by_select = "AVG(Rating) as Rating"
            sort_by_elements.append("Rating")
        elif param == "cocoa":
            sort_by_select = "AVG(CocoaPercent) as CocoaPercent"
            sort_by_elements.append("CocoaPercent")
        elif param == "bars_sold":
            sort_by_select = "Count(*) as bars_sold"
            sort_by_elements.append("bars_sold")
        elif param == "sellers":
            join_on = "Bars.CompanyLocationId"
        elif param == "sources":
            join_on = "Bars.BroadBeanOriginId"
        else:
            print("Command not recognized")
            return


    if not sort_by_elements:
        sort_by_select = "AVG(Rating) as Rating"
        sort_by_elements.append("Rating")
    if not limit_elements:
        limit_elements.append(["top", 10])
    if not where_clause_elements:
        where_clause = ""
    else:
        where_clause = "WHERE " + 'and'.join(where_clause_elements)

    sort_by_clause_top = "ORDER BY " + sort_by_elements[0] + " DESC"
    sort_by_clause_bottom = "ORDER BY " + sort_by_elements[0]
    group_by_clause = "GROUP BY C1.EnglishName"

    statement = """SELECT C1.EnglishName, C1.Region, %s
                        FROM Bars
                            JOIN Countries C1 ON C1.Id = %s
                            %s
                            %s
                            HAVING count(*) > 4
                            %s
                            """
    conn = sqlite3.connect('choc.db')
    cur = conn.cursor()
    limit_elements_clause = limit_elements[0]
    if 'bottom' in limit_elements_clause:
        cur.execute(statement % (sort_by_select, join_on,where_clause, group_by_clause, sort_by_clause_bottom))
    else:
        cur.execute(statement % (sort_by_select, join_on,where_clause, group_by_clause, sort_by_clause_top))
    table = cur.fetchall()
    result = table[:limit_elements_clause[1]]
    return result


def processRegions(parameters):
    '''
    This function will run if the user command is regions, and will process the
    parameters according to the given list to give desired result.
    '''
    possible_limit_by = ["top", "bottom"]
    sort_by_elements = []
    sort_by_select = []
    limit_elements = []
    join_on = "Bars.CompanyLocationId"
    for param in parameters:
        if "=" in param and param.split("=")[0] in possible_limit_by:
            key = param.split("=")[0]
            value = param.split("=")[1]
            limit_elements.append([key, int(value)])
        elif param == "ratings":
            sort_by_select = "AVG(Rating) as Rating"
            sort_by_elements.append("Rating")
        elif param == "cocoa":
            sort_by_select = "AVG(CocoaPercent) as CocoaPercent"
            sort_by_elements.append("CocoaPercent")
        elif param == "bars_sold":
            sort_by_select = "Count(*) as bars_sold"
            sort_by_elements.append("bars_sold")
        elif param == "sellers":
            join_on = "Bars.CompanyLocationId"
        elif param == "sources":
            join_on = "Bars.BroadBeanOriginId"
        else:
            print("Command not recognized")
            return

    if not sort_by_elements:
        sort_by_select = "AVG(Rating) as Rating"
        sort_by_elements.append("Rating")
    if not limit_elements:
        limit_elements.append(["top", 10])

    sort_by_clause_top = "ORDER BY " + sort_by_elements[0] + " DESC"
    sort_by_clause_bottom = "ORDER BY " + sort_by_elements[0]
    group_by_clause = "GROUP BY C1.Region"
    statement = """SELECT C1.Region, %s
                            FROM Bars
                                JOIN Countries C1 ON C1.Id = %s
                                %s
                                HAVING count(*) > 4
                                %s
                                """
    conn = sqlite3.connect('choc.db')
    cur = conn.cursor()
    limit_elements_clause = limit_elements[0]
    if 'bottom' in limit_elements_clause:
        cur.execute(statement % (sort_by_select, join_on, group_by_clause, sort_by_clause_bottom))
    else:
        cur.execute(statement % (sort_by_select, join_on, group_by_clause, sort_by_clause_top))
    table = cur.fetchall()
    result = table[:limit_elements_clause[1]]
    return result


def load_help_text():
    with open('help.txt') as f:
        return f.read()


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