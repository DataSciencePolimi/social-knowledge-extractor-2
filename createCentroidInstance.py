import pymongo
import sys
import json

def loginMongo():
    fileKeys = open('adressMongo.json').read()
    keys = json.loads(fileKeys)
    client = pymongo.MongoClient(keys['adress_local'],int(keys['port_local']))
    return client[keys['name_db']]

def getFeatureVectors(db, id_experiment):
    seed_features = []
    seeds = db['users'].find({'id_experiment':id_experiment, 'type':'seeds'},{'_id':0, 'instances':1})
    for i in seeds:
        features = i['instances']
        seed_features.append(features)
    return seed_features



def createCentroid(seed_features):
    centroid = {}
    for seed in seed_features:
        for feature in seed:
            if feature not in centroid:
                centroid[feature] = seed[feature]
            else:
                centroid[feature] += seed[feature]
    num_seeds = len(seed_features)
    for feature in centroid:
        centroid[feature] /= float(num_seeds)
    return centroid

def storeCentroid(db, centroid, id_experiment):
    collection='centroid'
    db[collection].insert({'centroid':centroid,'id_experiment':id_experiment, 'type':'instance'})

def main():
    #    do the mongo login
    try:
        db = loginMongo()
    except:
        print('error login Mongo')
    
    id_experiment = sys.argv[1:][0]
    seed_features = getFeatureVectors(db, id_experiment)
    centroid = createCentroid(seed_features)
    storeCentroid(db, centroid, id_experiment)

if __name__ == "__main__":
    main()

