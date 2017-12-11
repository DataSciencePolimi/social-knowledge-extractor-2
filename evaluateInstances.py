import sys
import json
import pymongo
from scipy import spatial

def loginMongo():
    fileKeys = open('adressMongo.json').read()
    keys = json.loads(fileKeys)
    client = pymongo.MongoClient(keys['adress_local'],int(keys['port_local']))
    return client[keys['name_db']]



def getCentroid(db, id_experiment):
    centroid = db['centroid'].find_one({'id_experiment':id_experiment, 'type':'instance'},{'centroid':1, '_id':0})['centroid']
    return centroid

def getCandidates(db, id_experiment):
    result = db['users'].find({'id_experiment':id_experiment, 'type':'candidates'},{'instances':1, 'screen_name':1})
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
    for f in user_features:
        vect_centroid.append(0)
        vect_user.append(user_features[f])
    
    score = 1 - spatial.distance.cosine(vect_centroid, vect_user)
    return score

#def storeScore(cursor, id_experiment, screen_name, score):
#    print(screen_name, id_experiment, score)
#    command = ("UPDATE candidates SET score="+str(score)+" WHERE id_experiment="+id_experiment+" AND screen_name='"+screen_name+"'")
#    cursor.execute(command)
def storeScore(db, id_experiment, screen_name, score):
    db['users'].update({'id_experiment':id_experiment, 'screen_name':screen_name},{'$set':{'score_instances':score}})

def main():
    #    do the mongo login
    try:
        dbMongo = loginMongo()
    except:
        print('error login Mongo')
    id_experiment = sys.argv[1:][0]

    centroid = getCentroid(dbMongo, id_experiment)

    list_candidates = getCandidates(dbMongo, id_experiment)

    for candidate in list_candidates:
        screen_name = candidate['screen_name']
        features = candidate['instances']
        score = evaluateCandidate(centroid, features)
        storeScore(dbMongo, id_experiment, screen_name, score)
    

if __name__ == "__main__":
    main()
