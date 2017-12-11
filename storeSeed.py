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

def storeSeed(cursor, data):
    add_seed = ('INSERT INTO seeds (screen_name, id_experiment) VALUES (%(screen_name)s, %(id_experiment)s)')
    cursor.execute(add_seed,data)


def main():
    try:
        dbSQL, cursor = loginMySql()
    except:
        print('error login MySQL')
    args = sys.argv[1:]
    fileAccounts = open(args[0],'r').readlines()
    id_experiment = args[1]
    for account in fileAccounts:
        storeSeed(cursor, {'screen_name':account.lower().replace('\n',''), 'id_experiment':id_experiment})
    dbSQL.commit()
    cursor.close()
    dbSQL.close()


if __name__ == "__main__":
    main()
