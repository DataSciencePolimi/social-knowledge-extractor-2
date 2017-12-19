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
#poi lo riscriviamo in inglese comunque ritorna la stringa grande chiamata all_tweets con tutti i tweets (separati come avevamo detto da '. ') e un dizionario con {id_tweet: indice della posizione dove finisce il tweet in all_tweets,...}
def getTweets(id_exp, mongo_client):
    print(mongo_client)
    collection = 'tweets'
    
    all_tweets = {"de":"", "en":"", "es":"", "fr":"", "it":"", "pt":""}
    
#prende tutti i tweet con l'id_esperiment passato in input
    tweets = mongo_client[collection].find({'id_experiment':{'$in':[str(id_exp)]},'annotation':{'$exists': False}})
# sarà la stringa che contiene tutti i tweet concatenati e separati da '. '
    #all_tweets = '' #the string of the concatenation ll tweets
    print(tweets.count)
# dizionario dove salviamo gli indici di dove finisce ogni tweet in all_tweets
    index_tweets = {"de":[], "en":[], "es":[], "fr":[], "it":[], "pt":[]} #the dictionary
    id_tweets ={"de":[], "en":[], "es":[], "fr":[], "it":[], "pt":[]}
# per vedere dove siamo arrivati
    count = 0
    for t in tweets:
        count+=1
        print(count)
        id_tweet = t['_id'] #id del tweet che stiamo analizzando
        text = t['text'] #testo del tweet che stiamo analizzando
        lang = t["lang"] #linguaggio del tweet che stiamo analizzando
        all_tweets[lang] += text+'. ' #concateniamo il testo del tweet che stiamo analizzando
        index_tweets[lang].append(len(all_tweets[lang]))
        id_tweets[lang].append(id_tweet)
#        index_tweets[lang][id_tweet] = len(all_tweets[lang]) #settiamo l'indice di fine del tweet che stiamo analizzando
    print(index_tweets)
    return all_tweets, index_tweets, id_tweets

def callDandelion(text, datatxt):
    return datatxt.nex(text, include='types')


def storeAnnotations(id_tweet, annotation, mongo_client):
    collection = 'tweets'
    tweet = mongo_client[collection].find_one({'_id':id_tweet})
    if 'annotation' not in tweet:
        tweet['annotation'] = annotation
        mongo_client[collection].update_one({'_id':id_tweet}, {"$set": {'annotation':annotation}}, upsert=False)

def findTypes(dbpedia, types, parent=None, l = []):
    root = list(dbpedia.keys())[0]
    if root in types:
        l.append(root)
        if parent != None and parent in types:
            types.remove(parent)
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
    ltypes = list(map(lambda x: x.replace('http://dbpedia.org/ontology/',''),ltypes))
    return ltypes

def binarySearch(index_tweets, item):
    first = 0
    last = len(index_tweets)-1
    found = False
    if item>index_tweets[last] or item<first:
        return -1
    while first<=last and not found:
        midpoint = (first + last)//2
        if index_tweets[midpoint] > item and index_tweets[midpoint-1]< item:
            found = midpoint
        else:
            if item < index_tweets[midpoint]:
                last = midpoint-1
            else:
                first = midpoint+1
    
    return midpoint

def addStart(list, start):
    for i in list:
        i['start']+=start
        i['end']+=start
    return list

def main():
    #input id_exp seed o candidate?

    MAX_QUERY = 1038476
    confidence = 0.
    args = sys.argv[1:]
    id_exp = str(args[0])
    # login dandelion
    datatxt = loginDandelion()

#    logine mongo
    mongo_client = loginMongo()

#get all tweets and a dictionary of the index of the end of each tweet
    all_tweets, index_tweets, id_tweets = getTweets(id_exp, mongo_client)
#    print(len(all_tweets))
#   create indexes for dandelion calls
#    capiamo quante chiamate a dandelion dobbiamo fare
    for lang in all_tweets:
        lenght = len(all_tweets[lang])
        if lenght == 0:
            continue
        size_tweets = sys.getsizeof(all_tweets[lang])
        numbOfCalls = math.ceil(size_tweets/MAX_QUERY)
        num_bytes = math.ceil(lenght/numbOfCalls)
        print(numbOfCalls)
        start = 0 #indice di inizio del sottotesto che dobbiamo inviare a dandelion
        annotations = []
        
        #dandelion
        for i in range(numbOfCalls):
            end = start + num_bytes #indice di fine del sottotesto che dobbiamo inviare a dandelion
            if (end> lenght): #nel caso in cui la fine è dopo la fine del nostro testo
                end = lenght
            result = datatxt.nex(all_tweets[lang][start: end], **{"lang": lang,"include": "types","social.hashtag": True,"social.mention": True,"min_confidence":confidence})
            new = addStart(result['annotations'],start)
            start = end
            annotations += new
        tweet_annotation = {}
#        keys = sorted(index_tweets[lang].items(), key=operator.itemgetter(1))

        for ann in annotations:
            start = int(ann['start']) #indice di inizio dell'annotazione nel testo grande
            end = int(ann['end']) #indice di fine dell'annotazione nel testo grande
#        salva risultati dandelion in mongo
#        ordiniamo il dizionario con gli indici di fine del tweet
#        salviamo l'indice di fine del precedente tweet (indice di inizio del tweet in cui c'è l'annotazione ann)
            prec = 0
#        indice del tweet in cui c'è l'annotazione ann
#        cerchiamo l'indice del tweet in cui c'è l'annotazione ann
            id_tweet = binarySearch(index_tweets[lang], start)
            if id_tweet != 0:
                prec = index_tweets[lang][id_tweet-1]
            elif id_tweet == -1:
                raise 'error in ann index'
            t = id_tweets[lang][id_tweet]
            ann['start'] = start- prec #modifichiamo l'inizio dell'annotazione togliendo la lunghezza del testo prima del tweet in cui c'è l'annotazione ann
            ann['end'] = end-prec #modifichiamo la fine dell'annotazione togliendo la lunghezza del testo prima del tweet in cui c'è l'annotazione ann
            
            if len(ann['types']) != 0:
                ann['types'] = getType(ann['types']) #prendiamo i type corretti (solo quelli più specifici
            if t not in tweet_annotation:
                tweet_annotation[t] = [ann]
            else:
                tweet_annotation[t].append(ann)
        for tw in tweet_annotation:
            storeAnnotations(tw,tweet_annotation[tw], mongo_client) #aggiorniamo il tweet in mongo
    print('end Dandelion',type)
    #fine :)



if __name__ == "__main__":
    main()
# 	mongo_client = loginMongo()
# 	getTweets("1", mongo_client)
#        for t in range(len(keys)):
##            l'annotazione è quando la fine del tweet è dopo l'inizio dell'annotazione
#            if int(keys[t][1]) > start:
#                id_tweet = keys[t][0]
#                break
#            else:
#               prec = keys[t][1] #aggiorniamo il precedente

