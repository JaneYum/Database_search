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
# create new database
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
conn.commit()
# create new table bars
statement = '''
    CREATE TABLE 'Bars' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Company' TEXT NOT NULL,
        'SpecificBeanBarName' TEXT,
        'REF' TEXT,
        'ReviewDate' TEXT,
        'CocoaPercent' REAL,
        'CompanyLocation' TEXT,
        'CompanyLocationId' INTEGER,
        'Rating' REAL,
        'BeanType' TEXT,
        'BroadBeanOrigin' TEXT,
        'BroadBeanOriginId' INTEGER
    );
'''
cur.execute(statement)
conn.commit()
# import CSV into this bars
ignore_header = False
with open(BARSCSV, newline='') as csvfile:
    bar = csv.reader(csvfile, delimiter=',', quotechar='"')
    i= 0
    for params in bar:
        if ignore_header:
            i+=1
            real = params[4].replace('%',' ').split(' ')
            real = float(real[0])/100.00
            rate = float(params[6])
            insertion = (i,params[0],params[1],params[2],params[3],real,params[5],i,rate,params[7],params[8],i)
            statement = 'INSERT INTO "Bars" '
            statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            cur.execute(statement, insertion)
            conn.commit()
        ignore_header = True

# create new table Countries
statement = '''
    CREATE TABLE 'Countries' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Alpha2' TEXT,
        'Alpha3' TEXT,
        'EnglishName' TEXT,
        'Region' TEXT,
        'Subregion' TEXT,
        'Population' INTEGER,
        'Area' REAL
    );
'''
cur.execute(statement)
conn.commit()

# import Json into this Countries
ignore_header = False
with open(COUNTRIESJSON) as json_data:
    d = json.load(json_data)
    i= 0
    for country in d:
        i+=1
        insertion = (i,country['alpha2Code'],country['alpha3Code'],country['name'],country['region'],country['subregion'],country['population'],country['area'])
        statement = 'INSERT INTO "Countries" '
        statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        cur.execute(statement, insertion)
        conn.commit()


# create relationships between two tables two foreign keys in the bars table

# CompanyLocation
# CompanyLocationId
# BroadBeanOrigin
# BroadBeanOriginId
# Countries.EnglishName
# Countries.Id
# Set Unknown Id
query = '''
        SELECT CompanyLocation,BroadBeanOrigin
        FROM Bars
'''
cur.execute(query)
location_list= cur.fetchall()
i = 0
for location in location_list:
    i += 1
    query = '''
            SELECT Countries.Id
            FROM Bars
                JOIN Countries
                ON Bars.CompanyLocation = Countries.EnglishName
            WHERE Bars.CompanyLocation = ?
    '''
    params = (location[0],)
    cur.execute(query, params)
    location_Ids= cur.fetchone()

    if location_Ids != None:
        params = (location_Ids[0],location[0])
        statement = '''
            UPDATE Bars
            SET CompanyLocationId = ?
            WHERE Bars.CompanyLocation = ?
        '''
    else:
        params = ('Unknown',location[0])
        statement = '''
            UPDATE Bars
            SET CompanyLocationId = ?
            WHERE Bars.CompanyLocation = ?
        '''
    cur.execute(statement, params)
    conn.commit()

    query = '''
            SELECT Countries.Id
            FROM Bars
                JOIN Countries
                ON Bars.BroadBeanOrigin = Countries.EnglishName
            WHERE Bars.BroadBeanOrigin = ?
    '''
    params = (location[1],)
    cur.execute(query, params)
    location_Ids= cur.fetchone()
    if location_Ids != None:
        params = (location_Ids[0],location[1])
        statement = '''
            UPDATE Bars
            SET BroadBeanOriginId = ?
            WHERE Bars.BroadBeanOrigin = ?
        '''
    else:
        params = ('Unknown',location[1])
        statement = '''
            UPDATE Bars
            SET BroadBeanOriginId = ?
            WHERE Bars.BroadBeanOrigin = ?
        '''
    cur.execute(statement, params)
    conn.commit()

    query = '''
            SELECT Countries.Id
            FROM Bars
                JOIN Countries
                ON Bars.CompanyLocation = Countries.EnglishName
            WHERE Bars.CompanyLocation = ?
    '''
    params = (location[0],)
    cur.execute(query, params)
    location_Ids= cur.fetchone()
    if location_Ids != None:
        params = (location_Ids[0],location[0])
        statement = '''
            UPDATE Bars
            SET CompanyLocationId = ?
            WHERE Bars.CompanyLocation = ?
        '''
    else:
        params = ('Unknown',location[0])
        statement = '''
            UPDATE Bars
            SET CompanyLocationId = ?
            WHERE Bars.CompanyLocation = ?
        '''
    cur.execute(statement, params)
    conn.commit()

conn.commit()

# Part 2: Implement logic to process user commands
def process_command(command):

    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    parameter = tuple(command.replace('=',' ').split(' '))

    results = []

    if parameter[0] == 'bars':
        statement = 'SELECT "SpecificBeanBarName","Company","CompanyLocation","Rating","CocoaPercent","BroadBeanOrigin" '
        statement += 'FROM "Bars" '
        if len(parameter)==1:
            statement += 'ORDER BY "Rating" DESC '
            statement += 'LIMIT 10 '
            cur.execute(statement)
            results= cur.fetchall()
            width = 10
            for result in results:
                output = '{:20.17} {:20.17} {:20.17} {:3.1f} {:1.2f} {:10s}'.format(result[0],result[1],result[2],result[3],result[4],result[5])
                print (output)
        else:
            i = 1
            for input in parameter[1:-1]:
                if input == 'sellcountry':
                    statement += '      JOIN "Countries" ON Countries.Alpha2 = ? '
                    statement += 'WHERE Bars.CompanyLocation = Countries.EnglishName '
                    i+=1
                if input == 'sourcecountry':
                    statement += '      JOIN "Countries" ON Countries.Alpha2 = ? '
                    statement += 'WHERE Bars.BroadBeanOrigin = Countries.EnglishName '
                    i+=1
                if input == 'sellregion':
                    statement += '      JOIN "Countries" ON Countries.Region = ? '
                    statement += 'WHERE Bars.CompanyLocation = Countries.EnglishName '
                    i+=1
                if input == 'sourceregion':
                    statement += '      JOIN "Countries" ON Countries.Region = ? '
                    statement += 'WHERE Bars.BroadBeanOrigin = Countries.EnglishName '
                    i+=1

            if 'ratings' not in parameter and 'cocoa' not in parameter:
                statement += 'ORDER BY "Rating" '

            for input in parameter[1:-1]:
                if input == 'ratings':
                    statement += 'ORDER BY "Rating" '
                    i+=1
                if input == 'cocoa':
                    statement += 'ORDER BY "CocoaPercent" '
                    i+=1
                if input == 'top':
                    statement += 'DESC '
                    statement += 'LIMIT ? '
                    i+=1
                if input == 'bottom':
                    statement += 'LIMIT ? '
                    i+=1

            if 'top' not in parameter and 'bottom' not in parameter:
                statement += 'DESC '
                statement += 'LIMIT 10 '

            if i == 1:
                cur.execute(statement)
            if i == 2:
                params = (parameter[-1],)
                cur.execute(statement,params)
            if i == 3:
                params = (parameter[-1],)
                cur.execute(statement,params)
            if i == 4:
                params = (parameter[2],parameter[-1])
                cur.execute(statement,params)

            results= cur.fetchall()
            for result in results:
                output = '{:20.17} {:20.17} {:20.17} {:3.1f} {:1.2f} {:10s}'.format(result[0],result[1],result[2],result[3],result[4],result[5])
                print (output)

    if parameter[0] == 'companies':
        # Only companies that sell more than 4 kinds of bars are listed in results.
        if len(parameter)==1:
            statement = 'SELECT "Company","CompanyLocation",AVG("Rating"),COUNT(*) '
            statement += 'FROM "Bars" '
            statement += 'GROUP BY "Company" '
            statement += 'HAVING COUNT(*) > 4 '
            statement += 'ORDER BY AVG("Rating") DESC '
            statement += 'LIMIT 10 '
            cur.execute(statement)
            results= cur.fetchall()
            print(results)
            for result in results:
                output = '{:20.17} {:20.17} {:3.1f}'.format(result[0],result[1],result[2])
                print (output)
        else:
            if 'ratings' not in parameter and 'cocoa' not in parameter and 'bars_sold' not in parameter :
                statement = 'SELECT "Company","CompanyLocation",avg(Rating),COUNT(*) '
                statement += 'FROM "Bars" '
            elif 'bars_sold' in parameter:
                statement = 'SELECT "Company","CompanyLocation",COUNT(*) '
                statement += 'FROM "Bars" '
            elif 'ratings' in parameter:
                statement = 'SELECT "Company","CompanyLocation",avg(Rating),COUNT(*) '
                statement += 'FROM "Bars" '
            else:
                statement = 'SELECT "Company","CompanyLocation",avg(CocoaPercent),COUNT(*) '
                statement += 'FROM "Bars" '

            for input in parameter[1:-1]:
                if input == 'country':
                    statement += '      JOIN "Countries" ON Countries.Alpha2 = ? '
                    statement += 'WHERE Bars.CompanyLocation = Countries.EnglishName '

                if input == 'region':
                    statement += '      JOIN "Countries" ON Countries.Region = ? '
                    statement += 'WHERE Bars.CompanyLocation = Countries.EnglishName '

            statement += 'GROUP BY "Company" '
            statement += 'HAVING COUNT(*) > 4 '

            if 'ratings' not in parameter and 'cocoa' not in parameter and 'bars_sold' not in parameter :
                statement += 'ORDER BY avg(Rating) '

            for input in parameter[1:]:
                if input == 'ratings':
                    statement += 'ORDER BY avg(Rating) '

                if input == 'cocoa':
                    statement += 'ORDER BY avg(CocoaPercent) '

                if input == 'bars_sold':
                    statement += 'ORDER BY COUNT(*) '

                if input == 'top':
                    statement += 'DESC '
                    statement += 'LIMIT ? '

                if input == 'bottom':
                    statement += 'LIMIT ? '

            if 'top' not in parameter and 'bottom' not in parameter:
                statement += 'DESC '
                statement += 'LIMIT 10 '

            if 'country' in parameter or 'region' in parameter:
                if 'top' in parameter or 'bottom' in parameter:
                    params = (parameter[2],parameter[-1])
                    cur.execute(statement,params)
                else:
                    params = (parameter[2],)
                    cur.execute(statement,params)
            else:
                if 'top' in parameter or 'bottom' in parameter:
                    params = (parameter[-1],)
                    cur.execute(statement,params)
                else:
                    cur.execute(statement)

            results= cur.fetchall()
            if 'bars_sold' in parameter:
                for result in results:
                    output = '{:20.17} {:20.17} {:3}'.format(result[0],result[1],result[2])
                    print (output)
            elif 'cocoa' in parameter:
                for result in results:
                    output = '{:20.17} {:20.17} {:3.2f}'.format(result[0],result[1],result[2])
                    print (output)
            else:
                for result in results:
                    output = '{:20.17} {:20.17} {:3.1f}'.format(result[0],result[1],result[2])
                    print (output)

    if parameter[0] == 'countries':

        if 'sellers' in parameter:
            statement = 'SELECT "CompanyLocation", "Region" ,'
        elif 'sources' in parameter:
            statement = 'SELECT "BroadBeanOrigin", "Region" ,'
        else:
            statement = 'SELECT "CompanyLocation", "Region" ,'

        if 'ratings' in parameter:
            statement += 'avg(Rating),COUNT(*) '
            statement += 'FROM "Bars" '
        elif 'cocoa' in parameter:
            statement += 'avg(CocoaPercent),COUNT(*) '
            statement += 'FROM "Bars" '
        elif 'bars_sold' in parameter:
            statement += 'COUNT(*) '
            statement += 'FROM "Bars" '
        else:
            statement += 'avg(Rating),COUNT(*) '
            statement += 'FROM "Bars" '

        if 'region' in parameter:
            statement += '      JOIN "Countries" ON Countries.Region = ? '
        else:
            statement += '      JOIN "Countries" '

        if 'sellers' in parameter:
            statement += 'WHERE Bars.CompanyLocation = Countries.EnglishName '
            statement += 'GROUP BY "CompanyLocation" '
        elif 'sources' in parameter:
            statement += 'WHERE Bars.BroadBeanOrigin = Countries.EnglishName '
            statement += 'GROUP BY "BroadBeanOrigin" '
        else:
            statement += 'WHERE Bars.CompanyLocation = Countries.EnglishName '
            statement += 'GROUP BY "CompanyLocation" '

        statement += 'HAVING COUNT(*) > 4 '

        if 'ratings'in parameter:
            statement += 'ORDER BY avg(Rating) '
        elif 'cocoa'in parameter:
            statement += 'ORDER BY avg(CocoaPercent) '
        elif 'bars_sold'in parameter:
            statement += 'ORDER BY COUNT(*) '
        else:
            statement += 'ORDER BY avg(Rating) '

        if 'top' in parameter:
            statement += 'DESC '
            statement += 'LIMIT ? '
        elif 'bottom' in parameter:
            statement += 'LIMIT ? '
        else:
            statement += 'DESC '
            statement += 'LIMIT 10 '

        if 'region' in parameter:
            if 'top' in parameter or 'bottom' in parameter:
                params = (parameter[2],parameter[-1])
                cur.execute(statement,params)
            else:
                params = (parameter[2],)
                cur.execute(statement,params)
        else:
            if 'top' in parameter or 'bottom' in parameter:
                params = (parameter[-1],)
                cur.execute(statement,params)
            else:
                cur.execute(statement)

        results= cur.fetchall()
        if 'bars_sold' in parameter:
            for result in results:
                output = '{:20.17} {:20.17} {:3}'.format(result[0],result[1],result[2])
                print (output)
        elif 'cocoa' in parameter:
            for result in results:
                output = '{:20.17} {:20.17} {:3.2f}'.format(result[0],result[1],result[2])
                print (output)
        else:
            for result in results:
                output = '{:20.17} {:20.17} {:3.1f}'.format(result[0],result[1],result[2])
                print (output)

    if parameter[0] == 'regions':
        statement = 'SELECT "Region", '

        if 'ratings' in parameter:
            statement += 'avg(Rating),COUNT(*) '
            statement += 'FROM "Bars" '
        elif 'cocoa' in parameter:
            statement += 'avg(CocoaPercent),COUNT(*) '
            statement += 'FROM "Bars" '
        elif 'bars_sold' in parameter:
            statement += 'COUNT(*) '
            statement += 'FROM "Bars" '
        else:
            statement += 'avg(Rating),COUNT(*) '
            statement += 'FROM "Bars" '

        statement += '      JOIN "Countries" ON Countries.Region <> "Unknown" '

        if 'sellers' in parameter:
            statement += 'WHERE Bars.CompanyLocation = Countries.EnglishName '
        elif 'sources' in parameter:
            statement += 'WHERE Bars.BroadBeanOrigin = Countries.EnglishName '
        else:
            statement += 'WHERE Bars.CompanyLocation = Countries.EnglishName '

        statement += 'GROUP BY "Region" '
        statement += 'HAVING COUNT(*) > 4 '

        if 'ratings'in parameter:
            statement += 'ORDER BY avg(Rating) '
        elif 'cocoa'in parameter:
            statement += 'ORDER BY avg(CocoaPercent) '
        elif 'bars_sold'in parameter:
            statement += 'ORDER BY COUNT(*) '
        else:
            statement += 'ORDER BY avg(Rating) '

        if 'top' in parameter:
            statement += 'DESC '
            statement += 'LIMIT ? '
        elif 'bottom' in parameter:
            statement += 'LIMIT ? '
        else:
            statement += 'DESC '
            statement += 'LIMIT 10 '

        print(statement)
        if 'top' in parameter or 'bottom' in parameter:
            params = (parameter[-1],)
            cur.execute(statement,params)
        else:
            cur.execute(statement)

        results= cur.fetchall()
        print(results)
        if 'bars_sold' in parameter:
            for result in results:
                output = '{:20.17} {:10}'.format(result[0],result[1])
                print (output)
        elif 'cocoa' in parameter:
            for result in results:
                output = '{:20.17} {:3.2f}'.format(result[0],result[1])
                print (output)
        else:
            for result in results:
                output = '{:20.17} {:3.1f}'.format(result[0],result[1])
                print (output)

    return results

def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')
        command = tuple(response.replace('=',' ').split(' '))
        if response == 'help':
            print(help_text)
            continue
        if command[0] not in ['bars','companies','countries','regions','help','exit']:
            print('Command not recognized: '+ response)
        else:
            process_command(response)
    if response =='exit':
        print('bye')
        exit()

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
