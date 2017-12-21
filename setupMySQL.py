import mysql.connector
import json
from mysql.connector import errorcode
def loginMySql():
    fileKeys = open('adressMySQL.json').read()
    keys = json.loads(fileKeys)
    client = mysql.connector.connect(user=keys["user"], password=keys["password"], host=keys["host"])
    cursor = client.cursor()
    return client, cursor, keys["database"]

def create_database(db_name, cursor):
    try:
        cursor.execute(
                       "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(db_name))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)
def create_tables(TABLES, cursor):
    for name in TABLES:
        try:
            ddl = TABLES[name]
            print("Creating table {}: ".format(name), end='')
            cursor.execute(ddl)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

def main():
    try:
        dbSQL, cursor, db_name = loginMySql()
    except:
        print('error login MySQL')
    TABLES = {}
    TABLES['seeds'] = (
                       "CREATE TABLE `seeds` ("
                       "  `screen_name` char(20) NOT NULL,"
                       "  `id_experiment` char(20) NOT NULL,"
                       " `id` int NOT NULL AUTO_INCREMENT,"
                       "PRIMARY KEY (`id`))")
    TABLES['hubs'] = (
                   "CREATE TABLE `hubs` ("
                   "  `screen_name` char(20) NOT NULL,"
                   "  `id_experiment` char(20) NOT NULL,"
                   " `id` int NOT NULL AUTO_INCREMENT,"
                   "PRIMARY KEY (`id`))")
    TABLES['candidates'] = (
                        "CREATE TABLE `candidates` ("
                        "  `screen_name` char(20) NOT NULL,"
                        "  `id_experiment` char(20) NOT NULL,"
                        " `score` float default NULL,"
                        " `id` int NOT NULL AUTO_INCREMENT,"
                            "PRIMARY KEY (`id`))")
    TABLES['emergents'] = (
                        "CREATE TABLE `emergents` ("
                        "  `screen_name` char(20) NOT NULL,"
                        "  `id_experiment` char(20) NOT NULL,"
                        " `id` int NOT NULL AUTO_INCREMENT,"
                           "PRIMARY KEY (`id`))")
    TABLES['validated'] = (
                        "CREATE TABLE `validated` ("
                        "  `screen_name` char(20) NOT NULL,"
                        "  `id_experiment` char(20) NOT NULL,"
                        " `score` float default NULL,"
                        " `id` int NOT NULL AUTO_INCREMENT,"
                           "PRIMARY KEY (`id`))")
    TABLES['expert_types'] = (
                       "CREATE TABLE `expert_types` ("
                       "  `type` char(20) NOT NULL,"
                       "  `id_experiment` char(20) NOT NULL,"
                       " `id` int NOT NULL AUTO_INCREMENT,"
                       "PRIMARY KEY (`id`))")
    
    try:
        dbSQL.database = db_name
        print('db already exists')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(db_name,cursor)
            dbSQL.database = db_name
        else:
            print(err)
            exit(1)
    print('eccomi')
    create_tables(TABLES, cursor)
    dbSQL.commit()
    cursor.close()
    dbSQL.close()

if __name__ == "__main__":
    main()


