import json
import pymongo
import getopt
import sys

def loginMongo():
    fileKeys = open('adressMongo.json').read()
    keys = json.loads(fileKeys)
    client = pymongo.MongoClient(keys['adress_local'],int(keys['port_local']))
    return client[keys['name_db']]

def findTypes(id_experiment, db):
    collection = 'tweets'
    tweets = db[collection].find({'id_experiment':id_experiment},{'annotation.types':1,'_id':0})
    space = set()
    for t in tweets:
        if 'annotation' in t:
            for i in t['annotation'][0]['types']:
                space.add(i)
    return space

def storeTypes(types, id_experiment, db):
    collection='space'
    db[collection].insert({'id_experiment':id_experiment, 'space':list(types)})

def main():
    try:
        db = loginMongo()
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
    types = findTypes(id_experiment, db)

    storeTypes(types,id_experiment, db)

if __name__ == "__main__":
    main()
