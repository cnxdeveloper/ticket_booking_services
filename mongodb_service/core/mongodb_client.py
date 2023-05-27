import pymongo
import urllib 
from logging_utils import LoggerBase
  
class MongoDBClient(LoggerBase):
    def __init__(self, hostname, port, user, pwd, name="MongoDBClient", root='./logs/'):
        super().__init__(name, root)
        self.username = urllib.parse.quote_plus(user)
        self.password = urllib.parse.quote_plus(pwd)
        self.host = 'mongodb://%s:%s@%s:%s/' % (self.username, self.password, hostname, port)
        self.logger.info("Connecting to {} !!!".format(self.host))
        self.client = pymongo.MongoClient(self.host, w=1)
        
    def db_is_exist(self, db_name:str):
        return db_name in self.client.list_database_names()
       
    def creat_db(self, db_name:str):
        if not self.db_is_exist(db_name):
            self.logger.info("Creat database {}".format(db_name))
            self.client[db_name]
    
    def collection_is_exist(self, db_name:str, collection_name:str):
        return collection_name in self.client[db_name].list_collection_names()
    
    def creat_collection(self, db_name:str, collection_name:str):
        if not self.collection_is_exist(db_name, collection_name):
            self.logger.info("Creat collection {} from database {}".format(collection_name, db_name))
            self.client[db_name][collection_name]
    
    def insert_one_dict(self, db_name:str, collection_name:str, dict_data:dict):
        with self.client.start_session() as session:
            with session.start_transaction():
                return self.client[db_name][collection_name].update_one(dict_data, upsert=True)