from pymongo import MongoClient

host = '0.0.0.0'
port = 27017
client = MongoClient(host=host, port=port)

db_name = 'enwiki'
collection_name = 'sentences'
collection =  client[db_name][collection_name]

query = {'links.title': 'Manche'}
print(collection.count_documents(query))
for sent in collection.find(query):
    print(sent)
