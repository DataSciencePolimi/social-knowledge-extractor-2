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

def getAccounts(cursor, id_experiment, name_table):
    command = ("SELECT screen_name FROM "+name_table+" WHERE id_experiment = "+id_experiment)
    cursor.execute(command)
    accounts = []
    for name in cursor:
        accounts.append(name[0])
    return accounts

def getAccountsVal(cursor, id_experiment, name_table):
    command = ("SELECT screen_name, score FROM "+name_table+" WHERE id_experiment = "+id_experiment)
    cursor.execute(command)
    accounts = []
    scores = []
    for name in cursor:
        accounts.append(name[0])
        scores.append(name[1])
    return accounts, scores

def storeValidated(val, score, id_experiment, cursor):
    command = ("INSERT INTO validated (screen_name, score, id_experiment) VALUES ('"+val+"', '"+score+"', '"+id_experiment+"')")
    cursor.execute(command)

def main():
    try:
        dbSQL, cursor = loginMySql()
    except:
        print('error login MySQL')
    args = sys.argv[1:]
    id_experiment = args[0]
    
    emergents = getAccounts(cursor, id_experiment, 'emergents')
    validated, scores = getAccountsVal(cursor, id_experiment, 'validated')
    precision = 0.
    for emerg in emergents:
        if emerg not in validated:
            print('https://twitter.com/'+emerg)
            score = input("score = ")
            storeValidated(emerg, score, id_experiment, cursor)
            precision += score
        else:
            precision += scores[validated.index(emerg)]
    print(len(emergents))
    print(precision)
    print('precision = ', precision/len(emergents))
    dbSQL.commit()
    cursor.close()
    dbSQL.close()


if __name__ == "__main__":
    main()
