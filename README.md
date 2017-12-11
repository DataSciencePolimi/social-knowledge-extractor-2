# README #

The Social Knowledge Extractor (SKE) is a software tool that allows to discover new entities using Twitter.
### INSTALLATION GUIDE ###
## Requirements ##
* Python (>3.4.0) and pip
* MongoDB
* MySQL
* A Dandelion Account
* A Twitter Application

## Setting up ##

* Download the repository
* Create 4 files for configuration:
  - addressMongo.json : setup of your Mongo, port, host and name_db: {"port_local": "", "adress_local": "", "name_db": "ske_2"}
  - addressMySQL.json : setup of your MySQL, password, user, host, database: {"password": "", "user": "", "host": "", "database": "ske_2"}
  - credentialsDandelion.json : your app_key and app_id (of your Dandelion account): {"app_key" : "", "app_id" : ""}
  - credentialsTwitter.json : your Twitter account : {"consumer_key": "", "access_token_secret": "", "access_token": "", "consumer_secret": ""}
 * create a csv file with the account names of your seeds, one seed name each row
 * setup on pipeline.sh the id of your experiment, the number of tweets to get for each user and the name of the file of your seeds

### Run ###

* from the terminal run pipeline.sh:
```bash pipeline.sh```


### Legenda ###


# storeSeed.py #
* takes as input a csv file of seed names and an id experiment
* write "seeds" in sql db

# twitter.py #
* takes as input parameters(N or dates), id experiment, type (seeds or candidates)
* write "tweets" and "users" in mongo db

# myDandelion.py #
* takes as input id experiment
* write annotations in "tweets" in mongo db

# createFeatureVector.py #
* takes as input id experiment and type
* write features in "users" in mongo db

# listCandidates.py #
* takes as input id experiment
* write "candidates" in sql db









## Databases (both called ske_2): ##
# SQL #
* * "seeds": screen_name, id_experiment
* * "candidates": screen_name, id_experiment, score
# Mongo #
* * "tweets": id_user, text, lang, favourite_count, reqteet_count, create_at, mentions, id_tweet, id_experiment, coordinates, annotations
* * "users": id_user, screen_name, id_experiment, type, features








