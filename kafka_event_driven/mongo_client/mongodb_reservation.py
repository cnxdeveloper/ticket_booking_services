from mongo_client.mongodb_client import MongoDBClient
import datetime
import pymongo
import time

class MongoDBReservation(MongoDBClient):
    def __init__(self, hostname, port, user, pwd, primary_key, name="MongoDBReservation", root='./logs/'):
        super().__init__(hostname, port, user, pwd, name, root)
        self.primary_key = primary_key
    
    def update_data(self, db_name, collection, data:dict):
        if not self.db_is_exist(db_name):
            self.create_db(db_name)
        if not self.collection_is_exist(db_name, collection):
            self.create_collection(db_name, collection)
            self.client[db_name][collection].create_index([(self.primary_key, 1)],background=True, unique=True)
        with self.client.start_session() as session:
            with session.start_transaction():
                data["update_time"] = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                self.client[db_name][collection].update_one( 
                                                            {self.primary_key:data[self.primary_key]},
                                                            {'$set':data},
                                                            upsert=True
                                                            )
                
    def delete_data(self, db_name, collecttion, data:dict):
        if not self.db_is_exist(db_name):
            return
        if not self.collection_is_exist(db_name, collecttion):
            return
        
        self.client[db_name][collecttion].delete_one(data)
        
    
    def get_data(self, db_name, collection, id, preload_db=None):
        self.logger.info("get data from database: {}, collection: {}, with ID {}".format(db_name, collection, id))
        if not self.db_is_exist(db_name):
            return None
        elif not self.collection_is_exist(db_name, collection):
            return None
        result = None
        if preload_db is None:
            cursor = self.client[db_name][collection].find({self.primary_key: id})
        else:
            cursor = preload_db[collection].find({self.primary_key: id})
        for rs in cursor:
            result = rs
        return result
    
    def drop_collection(self, db_name, collection):
        if not self.db_is_exist(db_name):
                return None
        elif not self.collection_is_exist(db_name, collection):
            return None
        self.client[db_name][collection].drop()
    
    def update_data_many(self, db_name, collection, list_data):
        self.logger.info("update data database: {}, collection: {}".format(db_name, collection))
        if not self.db_is_exist(db_name):
            self.creat_db(db_name)
        if not self.collection_is_exist(db_name, collection):
            self.creat_collection(db_name, collection)
        if len(list_data) == 0:
            return
        with self.client.start_session() as session:
            with session.start_transaction():
                collection_db = self.client[db_name][collection]
                collection_db.create_index([(self.primary_key, 1)],background=True, unique=True)
                print(collection_db.index_information())
                st = time.time()
                operations=[pymongo.UpdateOne({self.primary_key:data[self.primary_key]},{'$set':data},upsert=True) for data in list_data]
                print(time.time() - st)
                collection_db.bulk_write(operations)
    
    def __del__(self):
        self.client.close()
    