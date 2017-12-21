import json
import pymongo
import getopt
import sys

def loginMongo():
    fileKeys = open('adressMongo.json').read()
    keys = json.loads(fileKeys)
    client = pymongo.MongoClient(keys['adress_local'],int(keys['port_local']))
    return client[keys['name_db']]

def getUsers(id_experiment, type, db):
    collection = 'users'
    return db[collection].find({'id_experiment':id_experiment, 'type':type})

def createFeatures(id_experiment, users, db, N):
    for user in users:
        types = findTypes(id_experiment, user['id_user'], db, N)
        storeTypes(types, id_experiment, user['id_user'], db)

#storeTypes(types)


def findTypes(id_experiment,id_user, db, N):
    collection = 'tweets'
    if N != None:
        tweets = db[collection].find({'id_experiment':id_experiment, 'id_user':id_user},{'annotation.types':1,'_id':0}).sort([('create_at',1)]).limit(N)
    else:
        tweets = db[collection].find({'id_experiment':id_experiment, 'id_user':id_user},{'annotation.types':1,'_id':0})
    features = {}

    for t in tweets:
        if 'annotation' in t:
            for i in t['annotation']:
                for type in i['types']:
                    if type in features:
                        features[type] += 1
                    else:
                        features[type] = 1
#    print(features)
    return features

def storeTypes(types, id_experiment,id_user, db):
    collection='users'
    db[collection].update({'id_experiment':id_experiment,'id_user':id_user}, {'$set':{'features': types}})


def main():
    try:
        db = loginMongo()
    except:
        print('error login Mongo')
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'n:s:e:x:')
    except getopt.GetoptError as err:
        # print help information and exit:
        print('err')  # will print something like "option -a not recognized"
        #        usage()
        sys.exit(2)
    id_experiment = args[0]
    type = args[1]
    N = None
    if len(args)>2:
        N = int(args[2])
    users = getUsers(id_experiment, type, db)
    createFeatures(id_experiment, users, db, N)

if __name__ == "__main__":
    main()
