from dandelion import DataTXT
import sys
import json
import pymongo
import math
import operator
import mysql.connector
from mysql.connector import errors

#login dandelion
def loginDandelion():
    file = open('credentialsDandelion.json')
    keys = json.load(file)
    datatxt = DataTXT(app_id = keys['app_id'], app_key = keys['app_key'])
    return datatxt

def loginMySql():
    fileKeys = open('adressMySQL.json').read()
    keys = json.loads(fileKeys)
    try:
        client = mysql.connector.connect(user=keys["user"], password=keys["password"], host=keys["host"], database = keys["database"])
        cursor = client.cursor()
    except mysql.connector.errors.ProgrammingError:
        print("crea il db")
    return client, cursor

#login Mongo
def loginMongo():
    fileKeys = open('adressMongo.json').read()
    keys = json.loads(fileKeys)
    client = pymongo.MongoClient(keys['adress_local'],int(keys['port_local']))
    return client[keys['name_db']]

def storeCandidate(cursor, data):
    add_candidate = ('INSERT INTO candidates (screen_name, id_experiment) VALUES (%(screen_name)s, %(id_experiment)s)')
    cursor.execute(add_candidate,data)

def getSeeds(cursor, id_experiment):
    command = ("SELECT screen_name FROM seeds WHERE id_experiment = "+id_experiment)
    cursor.execute(command)
    accounts = []
    for name in cursor:
        accounts.append(name[0])
    return accounts


def getMentions(db,id_experiment, seeds):
    mentions =  db['tweets'].find({'id_experiment':{'$in':[str(id_experiment)]}},{'_id':0,'mentions':1})
    candidates = set()
    for m in mentions:
        for account in m['mentions']:
            screen_name = account['screen_name'].lower()
            if screen_name not in seeds:
                candidates.add(screen_name)
    return candidates


def main():
    args = sys.argv[1:]
    id_experiment = args[0]
    try:
        dbMongo = loginMongo()
    except:
        print('error login Mongo')
    try:
        dbSQL, cursor = loginMySql()
    except:
        print('error login MySQL')
    seeds = getSeeds(cursor, id_experiment)
    candidates = getMentions(dbMongo, id_experiment, seeds)
    for cand in candidates:
        print(cand)
        storeCandidate(cursor, {'screen_name':cand, 'id_experiment': id_experiment})
    dbSQL.commit()
    cursor.close()
    dbSQL.close()


if __name__ == "__main__":
    main()
