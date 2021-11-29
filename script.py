from pymongo import MongoClient
import csv


def ingest_from_csv(filename, collection_name):
    try:
            with open(filename, newline='') as csvfile:
                reader = csv.reader(csvfile, delimiter=',')
                entry = {}
                for idx, row in enumerate(reader):
                    if idx == 0:
                        continue
                    try:
                        entry = {
                            "COUNTRY" : row[0],
                            "2009" : row[1],
                            "2010" : row[2],
                            "2011" : row[3],
                            "2012" : row[4],
                            "2013" : row[5],
                            "2014" : row[6],
                            "2015" : row[7],
                            "2016" : row[8],
                            "2017" : row[9],
                            "2018" : row[10],
                            "2019" : row[11],
                            
                        }

                        collection_name.insert_one(entry)

                    except:
                        return

    except FileNotFoundError:
            print('File does not exist')

def get_database():
    CONNECTION_STRING = "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
    client = MongoClient(CONNECTION_STRING)
    return client['postnatalcare']

def print_collection(collection_name):   
    for item in collection_name.find():
        print(item)


if __name__ == "__main__":    
    dbname = get_database()
    collection_name = dbname['Post_natal_care']
    ingest_from_csv('PCD.csv', collection_name)
    print_collection(collection_name)
