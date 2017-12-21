import json
import sys
import pymongo
from pymongo import IndexModel, ASCENDING, DESCENDING


def loginMongo():
    fileKeys = open('adressMongo.json').read()
    keys = json.loads(fileKeys)
    client = pymongo.MongoClient(keys['adress_local'],int(keys['port_local']))
    return client[keys['name_db']]

def insertIndexes(db):
    names = db.collection_names()
    if len(names) == 0:
        db['users'].create_index([( 'id_experiment', ASCENDING), ('id_user', ASCENDING)])
        db['tweets'].create_index([( 'id_experiment', ASCENDING), ('id_tweet', ASCENDING)])


def main():
    client = loginMongo()
    insertIndexes(client)




if __name__ == "__main__":
    main()
