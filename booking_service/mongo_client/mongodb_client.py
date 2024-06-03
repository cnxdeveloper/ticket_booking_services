import pymongo
import urllib 
from logging_utils import LoggerBase
  
class MongoDBClient(LoggerBase):
    """
    A class representing a MongoDB client.
    """

    def __init__(self, hostname, port, user, pwd, name="MongoDBClient", root='./logs/'):
        """
        Initialize a MongoDBClient object.

        Args:
            hostname (str): The hostname of the MongoDB server.
            port (int): The port number of the MongoDB server.
            user (str): The username for authentication.
            pwd (str): The password for authentication.
            name (str, optional): The name of the MongoDBClient. Defaults to "MongoDBClient".
            root (str, optional): The root directory for logging. Defaults to './logs/'.
        """
        # Call the constructor of the parent class
        super().__init__(name, root)

        # URL encode the username and password
        self.username = urllib.parse.quote_plus(user)
        self.password = urllib.parse.quote_plus(pwd)

        # Construct the MongoDB connection string
        self.host = 'mongodb://%s:%s@%s:%s/' % (self.username, self.password, hostname, port)

        # Log the connection string
        self.logger.info("Connecting to {} !!!".format(self.host))

        # Create a MongoDB client
        self.client = pymongo.MongoClient(self.host, w=1)
        
    def db_is_exist(self, db_name:str):
        return db_name in self.client.list_database_names()
       
    def create_db(self, db_name:str):
        if not self.db_is_exist(db_name):
            self.logger.info("Creat database {}".format(db_name))
            self.client[db_name]
    
    def collection_is_exist(self, db_name:str, collection_name:str):
        return collection_name in self.client[db_name].list_collection_names()
    
    def create_collection(self, db_name:str, collection_name:str):
        if not self.collection_is_exist(db_name, collection_name):
            self.logger.info("Creat collection {} from database {}".format(collection_name, db_name))
            self.client[db_name][collection_name]
    
    def insert_one_dict(self, db_name:str, collection_name:str, dict_data:dict):
        with self.client.start_session() as session:
            with session.start_transaction():
                return self.client[db_name][collection_name].update_one(dict_data, upsert=True)