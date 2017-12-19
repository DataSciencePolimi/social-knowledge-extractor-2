import pymongo
import mysql.connector
from mysql.connector import errors
import sys
import json
from scipy import spatial

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




def storeScore(cursor, id_experiment, screen_name, score):
    print(screen_name, id_experiment, score)
    command = ("UPDATE candidates SET score="+str(score)+" WHERE id_experiment="+id_experiment+" AND screen_name='"+screen_name+"'")
    cursor.execute(command)

#type
def getCentroidType(db, id_experiment):
    centroid = db['centroid'].find_one({'id_experiment':id_experiment, 'type':'type'},{'centroid':1, '_id':0})['centroid']
    return centroid



def getCentroidInstance(db, id_experiment):
    centroid = db['centroid'].find_one({'id_experiment':id_experiment, 'type':'instance'},{'centroid':1, '_id':0})['centroid']
    return centroid

def getCandidates(db, id_experiment):
    result = db['users'].find({'id_experiment':id_experiment, 'type':'candidates'},{'instances':1,'features':1, 'screen_name':1})
    candidates =[]
    for cand in result:
        candidates.append(cand)
    return candidates

def evaluateCandidate(centroid, user_features):
    vect_centroid = []
    vect_user = []
    if len(user_features) == 0:
        return 0
    for i in centroid:
        vect_centroid.append(centroid[i])
        if i in user_features:
            vect_user.append(user_features[i])
            user_features.pop(i)
        else:
            vect_user.append(0)
#    for f in user_features:
#        vect_centroid.append(0)
#        vect_user.append(user_features[f])

    score = 1 - spatial.distance.cosine(vect_centroid, vect_user)
    return score



def getCentroid(centroidType, centroidInstance):
    alpha = 0.7
    totalCentroid = {}
    for i in centroidType:
        k = centroidType[i]
        totalCentroid[i] =k*(1-alpha)
    for i in centroidInstance:
        k = centroidInstance[i]
        totalCentroid[i] =k*(alpha)
    return totalCentroid

def getCandidateVector(types, instances):
    alpha = 0.7
    vector = {}
    for i in types:
        k = types[i]
        vector[i] = k*(1-alpha)
    for i in instances:
        k = instances[i]
        vector[i] = k*(alpha)
    return vector


def main():
    #    do the mongo login
    try:
        dbMongo = loginMongo()
    except:
        print('error login Mongo')
    try:
        dbSQL, cursor = loginMySql()
    except:
        print('error login MySQL')
    id_experiment = sys.argv[1:][0]

    centroidType = getCentroidType(dbMongo, id_experiment)

    centroidInstance = getCentroidInstance(dbMongo, id_experiment)

    centroid = getCentroid(centroidType, centroidInstance)

    list_candidates = getCandidates(dbMongo, id_experiment)

#    list_candidatesInstance = getCandidatesInstance(dbMongo, id_experiment)
    count = 0
#    for i in centroidType:
#        count+=centroidType[i]

    for candidate in list_candidates:
        user = candidate['screen_name']

        if 'feature' not in candidate or 'instances' not in candidate:
            print(user)
        types = candidate['features']
        instances = candidate['instances']
        for i in types:
            count+=types[i]
#        candidate_vector = getCandidateVector(types, instances)

#        score = evaluateCandidate(centroid, candidate_vector)
#        storeScore(cursor, id_experiment, user, score)
        print(count)

    dbSQL.commit()
    cursor.close()
    dbSQL.close()

if __name__ == "__main__":
    main()

