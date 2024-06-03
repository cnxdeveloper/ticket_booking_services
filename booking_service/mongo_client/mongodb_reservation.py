from mongo_client.mongodb_client import MongoDBClient
import datetime
import pymongo
import time

class MongoDBReservation(MongoDBClient):
    def __init__(self, hostname, port, user, pwd, primary_key, name="MongoDBReservation", root='./logs/'):
        super().__init__(hostname, port, user, pwd, name, root)
        self.primary_key = primary_key
    
    def update_data(self, db_name, collection, data:dict):
        """
        Updates a document in the specified database and collection.
        
        Args:
            db_name (str): The name of the database.
            collection (str): The name of the collection.
            data (dict): The data to be updated.
        """
        
        # Check if the database exists, if not create it
        if not self.db_is_exist(db_name):
            self.create_db(db_name)
        
        # Check if the collection exists, if not create it and create an index on the primary key
        if not self.collection_is_exist(db_name, collection):
            self.create_collection(db_name, collection)
            self.client[db_name][collection].create_index([(self.primary_key, 1)],background=True, unique=True)
        
        # Start a session and transaction and update the document
        with self.client.start_session() as session:
            with session.start_transaction():
                # Add the update time to the data
                data["update_time"] = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                
                # Update the document in the collection
                self.client[db_name][collection].update_one(
                    {self.primary_key:data[self.primary_key]}, # Filter the document to be updated
                    {'$set':data}, # Update the document with the new data
                    upsert=True # If the document is not found, insert a new document with the data
                )
                
                
    def delete_data(self, db_name, collection, data:dict):
        """
        Deletes a document from the specified database and collection.
        
        Args:
            db_name (str): The name of the database.
            collection (str): The name of the collection.
            data (dict): The data of the document to be deleted.
        """
        # Check if the database exists
        if not self.db_is_exist(db_name):
            return
        
        # Check if the collection exists
        if not self.collection_is_exist(db_name, collection):
            return
        
        # Delete the document from the collection
        self.client[db_name][collection].delete_one(data)

        
    
    def get_data(self, db_name, collection, id, preload_db=None):
        """
        Retrieves a document from the specified database and collection with the given ID.

        Args:
            db_name (str): The name of the database.
            collection (str): The name of the collection.
            id (str): The ID of the document to retrieve.
            preload_db (pymongo.database.Database, optional): A preloaded database object to use instead of creating a new one. Defaults to None.

        Returns:
            dict or None: The document with the given ID, or None if the document is not found.
        """
        # Log the operation
        self.logger.info("get data from database: {}, collection: {}, with ID {}".format(db_name, collection, id))
        
        # Check if the database exists
        if not self.db_is_exist(db_name):
            return None
        
        # Check if the collection exists
        if not self.collection_is_exist(db_name, collection):
            return None
        
        # Initialize the result variable
        result = None
        
        # Find the document with the given ID
        if preload_db is None:
            cursor = self.client[db_name][collection].find({self.primary_key: id})
        else:
            cursor = preload_db[collection].find({self.primary_key: id})
        for rs in cursor:
            result = rs
        
        # Return the result
        return result
    
    def drop_collection(self, db_name, collection):
        """
        Drops the specified collection from the database.

        Args:
            db_name (str): The name of the database.
            collection (str): The name of the collection to drop.

        Returns:
            None: If the database or collection does not exist.
        """
        # Check if the database exists
        if not self.db_is_exist(db_name):
            return None
        
        # Check if the collection exists
        elif not self.collection_is_exist(db_name, collection):
            return None
        
        # Drop the collection
        self.client[db_name][collection].drop()
    
    def update_data_many(self, db_name, collection, list_data):
        """
        Updates multiple documents in the specified database and collection.

        Args:
            db_name (str): The name of the database.
            collection (str): The name of the collection.
            list_data (list): A list of dictionaries representing the data to be updated.
        """
        # Log the update operation
        self.logger.info("update data database: {}, collection: {}".format(db_name, collection))

        # Check if the database exists, if not create it
        if not self.db_is_exist(db_name):
            self.creat_db(db_name)

        # Check if the collection exists, if not create it
        if not self.collection_is_exist(db_name, collection):
            self.creat_collection(db_name, collection)

        # Return if there is no data to update
        if len(list_data) == 0:
            return

        # Start a session and transaction
        with self.client.start_session() as session:
            with session.start_transaction():
                # Get the collection
                collection_db = self.client[db_name][collection]

                # Create an index on the primary key
                collection_db.create_index([(self.primary_key, 1)], background=True, unique=True)

                # Print the index information
                print(collection_db.index_information())

                # Measure the time it takes to create the operations
                st = time.time()

                # Create the update operations
                operations = [
                    pymongo.UpdateOne(
                        {self.primary_key: data[self.primary_key]},  # Filter the document to be updated
                        {'$set': data},  # Update the document with the new data
                        upsert=True  # If the document is not found, insert a new document with the data
                    ) for data in list_data
                ]

                # Print the time it took to create the operations
                print(time.time() - st)

                # Update the documents in the collection
                collection_db.bulk_write(operations)
    
    def __del__(self):
        self.client.close()
    