import mysql.connector
import sys
import json
from mysql.connector import errors


def loginMySql():
    fileKeys = open('adressMySQL.json').read()
    keys = json.loads(fileKeys)
    try:
        client = mysql.connector.connect(user=keys["user"], password=keys["password"], host=keys["host"], database = keys["database"])
        cursor = client.cursor()
    except mysql.connector.errors.ProgrammingError:
        print("crea il db")
    return client, cursor

def findTopCandidates(id_experiment, cursor):
    n = '20'
    command = ("SELECT screen_name FROM candidates WHERE id_experiment = "+id_experiment+" ORDER BY score DESC LIMIT "+n)
    cursor.execute(command)
    emergents = []
    for cand in cursor:
        emergents.append(cand[0])
    return emergents

def storeEmergents(emergent, id_experiment, cursor):
    command = ("INSERT INTO emergents (screen_name, id_experiment) VALUES ('"+emergent+"', '"+id_experiment+"')")
    cursor.execute(command)

def main():
    id_experiment = sys.argv[1:][0]
    try:
        dbSQL, cursor = loginMySql()
    except:
        print('error login MySQL')
    emergents = findTopCandidates(id_experiment, cursor)
    print(emergents)
    for em in emergents:
        storeEmergents(em, id_experiment, cursor)
    dbSQL.commit()
    cursor.close()
    dbSQL.close()

if __name__ == "__main__":
    main()
