import json
import pymongo
import getopt
import sys
import mysql.connector
from mysql.connector import errors

def loginMongo():
    fileKeys = open('adressMongo.json').read()
    keys = json.loads(fileKeys)
    client = pymongo.MongoClient(keys['adress_local'],int(keys['port_local']))
    return client[keys['name_db']]

def loginMySql():
    fileKeys = open('adressMySQL.json').read()
    keys = json.loads(fileKeys)
    try:
        client = mysql.connector.connect(user=keys["user"], password=keys["password"], host=keys["host"], database = keys["database"])
        cursor = client.cursor()
    except mysql.connector.errors.ProgrammingError:
        print("crea il db")
    return client, cursor

def getUsers(id_experiment, type, db):
    collection = 'users'
    return db[collection].find({'id_experiment':id_experiment, 'type':type})

def getExpertTypes(id_experiment, cursor):
    command = ("SELECT type FROM expert_types WHERE id_experiment='"+id_experiment+"'")
    cursor.execute(command)
    expert_types = []
    for c in cursor:
        expert_types.append(c[0])
    return expert_types

def createFeatures(id_experiment, users, expert_types, db):
    for user in users:
        instances = findInstances(id_experiment, user['id_user'], expert_types, db)
        storeInstances(instances, id_experiment, user['id_user'], db)

#storeTypes(types)


def findInstances(id_experiment,id_user, expert_types, db):
    collection = 'tweets'
    tweets = db[collection].find({'id_experiment':id_experiment, 'id_user':id_user},{'annotation.types':1,'_id':0, 'annotation.uri':1})
    instances = {}
#    print(expert_types)
    for t in tweets:
        if 'annotation' in t:
            for i in t['annotation']:
                for type in i['types']:
                    if type in expert_types:
                        inst = i['uri'].split('/')[-1].replace('.','')
                        if inst in instances:
                            instances[inst] += 1
                        else:
                            instances[inst] = 1
                        break
    return instances

def storeInstances(instances, id_experiment,id_user, db):
    collection='users'
    db[collection].update({'id_experiment':id_experiment,'id_user':id_user}, {'$set':{'instances': instances}})


def main():
    try:
        db = loginMongo()
    except:
        print('error login Mongo')
    try:
        dbSQL, cursor = loginMySql()
    except:
        print('error login Mongo')
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'x:')
    except getopt.GetoptError as err:
        # print help information and exit:
        print('err')  # will print something like "option -a not recognized"
        #        usage()
        sys.exit(2)
    id_experiment = args[0]
    type = args[1]

    users = getUsers(id_experiment, type, db)
    expert_types = getExpertTypes(id_experiment, cursor)
    
    createFeatures(id_experiment, users, expert_types, db)
    dbSQL.commit()
    cursor.close()
    dbSQL.close()

if __name__ == "__main__":
    main()

