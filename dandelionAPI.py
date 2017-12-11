from dandelion import DataTXT
import sys
import json
import pymongo
import math
import operator

#login dandelion
def loginDandelion():
    file = open('credentialsDandelion.json')
    keys = json.load(file)
    datatxt = DataTXT(app_id = keys['app_id'], app_key = keys['app_key'])
    return datatxt


#login Mongo
def loginMongo():
    fileKeys = open('adressMongo.json').read()
    keys = json.loads(fileKeys)
    client = pymongo.MongoClient(keys['adress_local'],int(keys['port_local']))
    return client[keys['name_db']]

#return a string with all tweets and a dictionary made in this way:
#{tweet: end index}
def getTweets(id_exp, mongo_client):
    collection = 'tweets'
    #    get all tweets for the experiment id_exp and order by id_user
    tweets = mongo_client[collection].find({'id_experiment':{'$in':[str(id_exp)]}})
    print(tweets)
    all_tweets = '' #the string of the concatenation ll tweets
    
    index_tweets = {} #the dictionary
    
    last = len(all_tweets)
    user = str(tweets[0]['id_user'])
    
    index_tweets = {}
    
    for tweet in tweets:
        #        set indexes of end of tweet
        id_tweet = tweet['_id']
        text = tweet['text']
        last += 2+ len(text)
        index_tweets[id_tweet]= last
        all_tweets += text+' .'
    
    
    return all_tweets, index_tweets

def callDandelion(text, datatxt):
    return datatxt.nex(text, include='types')


def storeAnnotations(id_tweet, annotation, mongo_client):
    collection = 'tweets'
    tweet = mongo_client[collection].find_one({'_id':id_tweet})
    if 'annotations' not in tweet:
        tweet['annotation'] = [annotation]
    else:
        tweet['annotation'] += [annotation]
    mongo_client[collection].update_one({'_id':id_tweet}, {"$set": tweet}, upsert=False)
#    mongo_client[collection].insert(annotation)

#def findTypes(dbpedia, types, index=0):
#    if isinstance(dbpedia, dict):
#        for t in dbpedia:
#            if t in types:
#                types[t] = index
#            types = findTypes(dbpedia[t], types, index+1)
#    else:
#        if dbpedia in types:
#            types[dbpedia] = index
#    return types

def findTypes(dbpedia, types, parent=None, l = []):
    root = list(dbpedia.keys())[0]
    if root in types:
        l.append(root)
        if parent != None and parent in types:
            #        print(parent, root)
            types.remove(parent)
            l.remove(parent)
        parent = root
    for t in dbpedia[root]:
        dict = {t:dbpedia[root][t]}
        findTypes(dict, types, parent, l)
    return types, l

def getType(types):
    dbpedia = json.load(open('dbpedia_ontology.json'))
    ltypes, l = findTypes(dbpedia, types)
    for t in ltypes:
        if t not in l:
            ltypes.remove(t)
    return ltypes



def main():
    #input id_exp seed o candidate?
    
    MAX_QUERY = 1048476
    args = sys.argv[1:]
    id_exp = str(args[0])
    isSeed = args[1]
    # login dandelion
    datatxt = loginDandelion()
    
    #    logine mongo
    mongo_client = loginMongo()
    
    #get all tweets and a dictionary of the index of the end of each tweet
    all_tweets, index_tweets = getTweets(id_exp, mongo_client)
    
    #   create indexes for dandelion calls
    numbOfCalls = math.ceil(len(all_tweets)/MAX_QUERY)
    last = 0
    annotations = []
    
    #dandelion
    #ciclo for sugli user aggiungendo ad un testo totale fino a quando non superi 1048476 salvandoti indice di quando cambi user
    for i in range(numbOfCalls):
        index = last + MAX_QUERY
        if (index> len(all_tweets)):
            index = len(all_tweets)
        #        chiamata a dandelion
        result = callDandelion(all_tweets[last: index], datatxt)
        last = index
        annotations += result['annotations']
    
    
    
        for ann in annotations:
            start = int(ann['start'])
            end = int(ann['end'])
            
            
            #    salva risultati dandelion in mongo
            prec = list(index_tweets.keys())[0]
            for t in index_tweets:
                if int(index_tweets.get(t)) > start:
                    break
                else:
                    prec = t
            ann['start'] = start-(index_tweets.get(prec))
            ann['end'] = end-(index_tweets.get(prec))
            if len(ann['types']) != 0:
                ann['types'] = getType(ann['types'])
            #        ann['id_user'] = users[current_user_index]
            storeAnnotations(t,ann, mongo_client)

#fine :)



if __name__ == "__main__":
    main()



