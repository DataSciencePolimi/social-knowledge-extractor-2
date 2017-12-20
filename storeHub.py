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

def storeHub(cursor, screen_name, id_experiment):
    add_seed = ('INSERT INTO hubs (screen_name, id_experiment) VALUES ("'+screen_name+'", "'+str(id_experiment)+'")')
    cursor.execute(add_seed)

def getAccounts(cursor, id_experiment, name_table):
    command = ("SELECT screen_name FROM "+name_table+" WHERE id_experiment = "+id_experiment)
    cursor.execute(command)
    accounts = []
    for name in cursor:
        accounts.append(name[0])
    return accounts

def main():
    try:
        dbSQL, cursor = loginMySql()
    except:
        print('error login MySQL')
    args = sys.argv[1:]
    fileAccounts = open(args[0],'r').readlines()
    id_experiment = args[1]
    
    hubs = getAccounts(cursor, id_experiment, 'hubs')
    for account in fileAccounts:
        screen_name = account.lower().replace('\n','').replace(' ','')
        if screen_name not in hubs:
            storeHub(cursor, screen_name, id_experiment)
    dbSQL.commit()
    cursor.close()
    dbSQL.close()


if __name__ == "__main__":
    main()

