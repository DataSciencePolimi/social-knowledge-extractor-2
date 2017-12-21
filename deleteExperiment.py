import tweepy
import json
import sys
import getopt
from langdetect import detect
import pymongo
import mysql.connector




def loginMySql():
    fileKeys = open('adressMySQL.json').read()
    keys = json.loads(fileKeys)
    try:
        client = mysql.connector.connect(user=keys["user"], password=keys["password"], host=keys["host"], database = keys["database"])
        cursor = client.cursor()
    except mysql.connector.errors.ProgrammingError:
        print("crea il db")
    return client, cursor

def loginMongo():
    fileKeys = open('adressMongo.json').read()
    keys = json.loads(fileKeys)
    client = pymongo.MongoClient(keys['adress_local'],int(keys['port_local']))
    return client[keys['name_db']]
#prendere tweet accounts
def deleteMongo(db, id_experiment):
    db['users'].delete_many({'id_experiment':id_experiment})
    tweets = db['tweets'].find({'id_experiment':id_experiment})
    for t in tweets:
        t['id_experiment'] = t['id_experiment'].remove(id_experiment)
        if len(t['id_experiment']) == 0:
            db['tweets'].delete_one({'_id':t['_id']})
        db['tweets'].update_one({'_id':t['_id']}, {"$set": t}, upsert= False)

def deleteMySql(cursor, id_experiment):
    command = ("DELETE FROM seeds WHERE id_experiment = "+id_experiment)
    command1 = ("DELETE FROM candidates WHERE id_experiment = "+id_experiment)
    command2 = ("DELETE FROM expert_types WHERE id_experiment = "+id_experiment)
    command3 = ("DELETE FROM hubs WHERE id_experiment = "+id_experiment)
    command4 = ("DELETE FROM validated WHERE id_experiment = "+id_experiment)
    command5 = ("DELETE FROM emergents WHERE id_experiment = "+id_experiment)
    cursor.execute(command)
    cursor.execute(command1)
    cursor.execute(command2)
    cursor.execute(command3)
    cursor.execute(command4)
    cursor.execute(command5)



def main():
    #    do the mongo login
    try:
        db = loginMongo()
    except:
        print('error login Mongo')
    #        scrivi errore nel log
    try:
        dbSQL, cursor = loginMySql()
    except:
        print('error login MySQL')
    #    open accounts file
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'n:s:e:x:')
    except getopt.GetoptError as err:
        # print help information and exit:
        print('err')  # will print something like "option -a not recognized"
        #        usage()
        sys.exit(2)
    id_experiment = args[0]
    deleteMySql(cursor, id_experiment)
    deleteMongo(db, id_experiment)
    dbSQL.commit()
    cursor.close()
    dbSQL.close()

if __name__ == "__main__":
    main()
