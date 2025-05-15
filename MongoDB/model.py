from pymongo import MongoClient
from bson import ObjectId

# Conexión global
client = None
db = None
collection = None

def connect_and_initialize():
    global client, db, collection
    client = MongoClient("mongodb://localhost:27017/")
    db = client["social_profiles_db"]
    collection = db["profiles"]
    print("Conectado a MongoDB y colección 'profiles' lista.")
    return db

# Funciones CRUD (usarán la colección ya inicializada)
def get_all_profiles():
    return list(collection.find())

def get_profile_by_id(profile_id):
    return collection.find_one({"_id": ObjectId(profile_id)})

def get_profiles_by_user(user_id):
    return list(collection.find({"user_id": ObjectId(user_id)}))

def insert_profile(profile_data):
    profile_data["_id"] = ObjectId()
    profile_data["user_id"] = ObjectId(profile_data["user_id"])
    return collection.insert_one(profile_data).inserted_id

def update_profile(profile_id, new_data):
    return collection.update_one({"_id": ObjectId(profile_id)}, {"$set": new_data})

def delete_profile(profile_id):
    return collection.delete_one({"_id": ObjectId(profile_id)})
