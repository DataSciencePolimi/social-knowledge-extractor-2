import pymongo
import mysql.connector
from mysql.connector import errors
import sys
import json

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

def getScores(id_experiment, db):
    result = db['users'].find({'id_experiment':id_experiment, 'type':'candidates'},{'score_instances':1,'score_types':1,'_id':0,'screen_name':1})
    users_score = []
    for i in result:
        users_score.append(i)
    return users_score

def totalScore(user_score):
    alpha = 0.7
    score_instances = user_score['score_instances']
    score_types = user_score['score_types']
    total_score = (1-alpha)*score_types + alpha*score_instances
    return total_score

def storeScore(cursor, id_experiment, screen_name, score):
    print(screen_name, id_experiment, score)
    command = ("UPDATE candidates SET score="+str(score)+" WHERE id_experiment="+id_experiment+" AND screen_name='"+screen_name+"'")
    cursor.execute(command)

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
    users_score = getScores(id_experiment, dbMongo)
    for user in users_score:
        total_score = totalScore(user)
        storeScore(cursor, id_experiment, user['screen_name'], total_score)
    dbSQL.commit()
    cursor.close()
    dbSQL.close()


if __name__ == "__main__":
    main()

