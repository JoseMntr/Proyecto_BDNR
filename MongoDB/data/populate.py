import pandas as pd
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime

# Conexión a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["social_profiles_db"]
collection = db["profiles"]

# Leer CSV
df = pd.read_csv("data/MOCK_DATA.csv")

# Función para transformar cada fila
def transform_row(row):
    return {
        "_id": ObjectId(row["_id"]),
        "user_id": ObjectId(row["user_id"]),
        "username": row["username"],
        "social_media": row["social_media"],
        "number_of_posts": int(row["number_of_posts"]),
        "number_of_followers": int(row["number_of_followers"]),
        "number_of_follows": int(row["number_of_follows"]),
        "profile_picture": row["profile_picture"],
        "log_in_times": int(row["log_in_times"]),
        "preferences": {
            "preferred_types": row["preferences.preferred_types"],
            "notification_settings": {
                "email": row["preferences.notification_settings.email"] == "True",
                "push": row["preferences.notification_settings.push"] == "True"
            }
        },
        "created_at": datetime.strptime(row["created_at"], "%m/%d/%Y"),
        "updated_at": datetime.strptime(row["updated_at"], "%m/%d/%Y")
    }

# Transformar e insertar
documents = [transform_row(row) for _, row in df.iterrows()]
collection.insert_many(documents)

print(f"Inserted {len(documents)} documents into MongoDB.")
