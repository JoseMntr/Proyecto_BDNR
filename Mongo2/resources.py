#!/usr/bin/env python3
import falcon
from bson.objectid import ObjectId
from datetime import datetime


class ProfileResource:
    def __init__(self, db):
        self.collection = db.profiles

    async def on_get(self, req, resp, profile_id):
        """GET un solo perfil por su ID"""
        try:
            profile = self.collection.find_one({"_id": ObjectId(profile_id)})
            if profile:
                profile["_id"] = str(profile["_id"])
                profile["user_id"] = str(profile["_id"])

                # Convierte datetime a string si existen
                if "created_at" in profile:
                    profile["created_at"] = profile["created_at"].isoformat()
                if "updated_at" in profile:
                    profile["updated_at"] = profile["updated_at"].isoformat()

                resp.media = profile
                resp.status = falcon.HTTP_200
            else:
                resp.status = falcon.HTTP_404


        except Exception:
            raise falcon.HTTPBadRequest("Invalid profile_id format.")

    async def on_put(self, req, resp, profile_id):
        """PUT para actualizar un perfil"""
        try:
            data = await req.media
            data["updated_at"] = datetime.utcnow()
            result = self.collection.update_one(
                {"_id": ObjectId(profile_id)}, {"$set": data}
            )
            if result.matched_count > 0:
                # Convierte el _id a string
                data["_id"] = str(profile_id)
                # Convierte updated_at a string si existe
                if "updated_at" in data:
                    data["updated_at"] = data["updated_at"].isoformat()
                resp.media = data
                resp.status = falcon.HTTP_200
            else:
                resp.status = falcon.HTTP_404

        except Exception:
            raise falcon.HTTPBadRequest(description="Invalid data or profile_id format.")



    async def on_delete(self, req, resp, profile_id):
        """DELETE para eliminar un perfil"""
        try:
            result = self.collection.delete_one({"_id": ObjectId(profile_id)})
            if result.deleted_count > 0:
                resp.status = falcon.HTTP_200
            else:
                resp.status = falcon.HTTP_404
        except Exception:
            raise falcon.HTTPBadRequest("Invalid profile_id format.")


class ProfilesResource:
    def __init__(self, db):
        self.collection = db.profiles

    async def on_get(self, req, resp):
        """GET todos los perfiles o filtrados por social_media/user_id/username"""
        social_media = req.get_param("social_media")
        user_id = req.get_param("user_id")
        username = req.get_param("username")   # <-- AGREGA ESTA LÍNEA

        query = {}
        if social_media:
            query["social_media"] = social_media
        if user_id:
            try:
                query["user_id"] = ObjectId(user_id)
            except Exception:
                raise falcon.HTTPBadRequest("Invalid user_id format.")
        if username:                           # <-- Y ESTA LÍNEA
            query["username"] = username

        profiles = self.collection.find(query)
        profiles_list = []
        for profile in profiles:
            profile["_id"] = str(profile["_id"])
            profile["user_id"] = str(profile["_id"])  # si ahora usas user_id como _id

            # Convierte datetime a string si existen
            if "created_at" in profile:
                profile["created_at"] = profile["created_at"].isoformat()
            if "updated_at" in profile:
                profile["updated_at"] = profile["updated_at"].isoformat()
            
            profiles_list.append(profile)
        resp.media = profiles_list
        resp.status = falcon.HTTP_200


    async def on_post(self, req, resp):
        data = await req.media
        data = validate_profile_data(data)
        data["created_at"] = datetime.utcnow()
        data["updated_at"] = datetime.utcnow()

        # Solo usa user_id como _id si viene
        if "user_id" in data:
            data["_id"] = ObjectId(data["user_id"])
            del data["user_id"]

        result = self.collection.insert_one(data)
        data["_id"] = str(result.inserted_id)

        # Convierte datetime a string para respuesta
        if "created_at" in data:
            data["created_at"] = data["created_at"].isoformat()
        if "updated_at" in data:
            data["updated_at"] = data["updated_at"].isoformat()

        resp.media = data
        resp.status = falcon.HTTP_201

    
    async def on_get_total_posts(self, req, resp):
        username = req.get_param("username")
        if not username:
            raise falcon.HTTPBadRequest(description="Missing username")
        pipeline = [
            {"$match": {"username": username}},
            {"$group": {"_id": "$username", "total_posts": {"$sum": "$number_of_posts"}}}
        ]
        result = list(self.collection.aggregate(pipeline))
        if result:
            resp.media = {"username": username, "total_posts": result[0]["total_posts"]}
        else:
            resp.media = {"username": username, "total_posts": 0}

    
    async def on_get_rank_by_engagement(self, req, resp):
        username = req.get_param("username")
        match = {}
        if username:
            match["username"] = username

        pipeline = [
            {"$match": match} if match else {"$match": {}},
            {"$addFields": {
                "engagement_score": {
                    "$add": [
                        {"$ifNull": ["$number_of_followers", 0]},
                        {"$ifNull": ["$number_of_follows", 0]},
                        {"$ifNull": ["$number_of_posts", 0]}
                    ]
                }
            }},
            {"$sort": {"engagement_score": -1}}
        ]
        result = list(self.collection.aggregate(pipeline))
        # Serializa ObjectId y datetime
        for p in result:
            p["_id"] = str(p["_id"])
            if "created_at" in p:
                p["created_at"] = p["created_at"].isoformat()
            if "updated_at" in p:
                p["updated_at"] = p["updated_at"].isoformat()
        resp.media = result












# Tipos esperados de los campos del perfil
profile_fields = {
    "user_id": str,  # se convertirá a ObjectId
    "username": str,
    "social_media": str,
    "number_of_posts": int,
    "number_of_followers": int,
    "number_of_follows": int,
    "profile_picture": str,
    "log_in_times": int,
    "preferences": dict,
}


def validate_profile_data(data):
    for field, expected_type in profile_fields.items():
        if field not in data:
            # Solo lanza error si el campo es requerido
            # Si quieres que user_id sea opcional:
            if field == "user_id":
                continue
            raise falcon.HTTPBadRequest(description=f"Missing field: {field}")
        try:
            if expected_type is int:
                data[field] = int(data[field])
            elif expected_type is dict:
                if not isinstance(data[field], dict):
                    raise ValueError
            else:
                data[field] = expected_type(data[field])
        except (ValueError, TypeError):
            raise falcon.HTTPBadRequest(
                description=f"Invalid type for {field}: expected {expected_type.__name__}"
            )

    # Solo intenta convertir user_id si lo mandaron
    if "user_id" in data:
        try:
            data["user_id"] = ObjectId(data["user_id"])
        except Exception:
            raise falcon.HTTPBadRequest(
                description="Invalid format for user_id, must be ObjectId string."
            )

    # Validar estructura de preferences.notification_settings
    preferences = data["preferences"]
    if "preferred_types" not in preferences or not isinstance(
        preferences["preferred_types"], list
    ):
        raise falcon.HTTPBadRequest(
            description="Missing or invalid preferences.preferred_types (should be list)."
        )

    notif = preferences.get("notification_settings")
    if not isinstance(notif, dict) or not all(k in notif for k in ["email", "push"]):
        raise falcon.HTTPBadRequest(
            description="Invalid preferences.notification_settings structure."
        )

    if not isinstance(notif["email"], bool) or not isinstance(notif["push"], bool):
        raise falcon.HTTPBadRequest(
            description="preferences.notification_settings values must be boolean."
        )

    return data

