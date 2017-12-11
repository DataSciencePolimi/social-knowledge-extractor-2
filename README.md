# README #

This README would normally document whatever steps are necessary to get your application up and running.

### What is this repository for? ###

* Quick summary
* Version
* [Learn Markdown](https://bitbucket.org/tutorials/markdowndemo)

### How do I get set up? ###

* Summary of set up
* Configuration
* Dependencies
* Database configuration
* How to run tests
* Deployment instructions

### Contribution guidelines ###

* Writing tests
* Code review
* Other guidelines

### Who do I talk to? ###

* Repo owner or admin
* Other community or team contact


storeSeed.py
* takes as input a csv file of seed names and an id experiment
* write "seeds" in sql db

twitter.py
* takes as input parameters(N or dates), id experiment, type (seeds or candidates)
* write "tweets" and "users" in mongo db

myDandelion.py
* takes as input id experiment
* write annotations in "tweets" in mongo db

createFeatureVector.py
* takes as input id experiment and type
* write features in "users" in mongo db

listCandidates.py
* takes as input id experiment
* write "candidates" in sql db









Databases (both called ske_2):
* SQL
* * "seeds": screen_name, id_experiment
* * "candidates": screen_name, id_experiment, score
* Mongo
* * "tweets": id_user, text, lang, favourite_count, reqteet_count, create_at, mentions, id_tweet, id_experiment, coordinates, annotations
* * "users": id_user, screen_name, id_experiment, type, features








