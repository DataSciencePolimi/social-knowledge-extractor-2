import json
import mysql.connector
import sys

def loginMySql():
    fileKeys = open('adressMySQL.json').read()
    keys = json.loads(fileKeys)
    try:
        client = mysql.connector.connect(user=keys["user"], password=keys["password"], host=keys["host"], database = keys["database"])
        cursor = client.cursor()
    except mysql.connector.errors.ProgrammingError:
        print("do setup MySQL")
    return client, cursor

def storeType(cursor, data):
    add_seed = ('INSERT INTO expert_types (type, id_experiment) VALUES (%(type)s, %(id_experiment)s)')
    cursor.execute(add_seed,data)


def main():
    try:
        dbSQL, cursor = loginMySql()
    except:
        print('error login MySQL')
    args = sys.argv[1:]
    fileType = open(args[0],'r').readlines()
    id_experiment = args[1]
    for type in fileType:
        storeType(cursor, {'type':type.replace('\n',''), 'id_experiment':id_experiment})
    dbSQL.commit()
    cursor.close()
    dbSQL.close()


if __name__ == "__main__":
    main()
