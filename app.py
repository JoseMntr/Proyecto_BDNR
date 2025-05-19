# app.py

import falcon.asgi
import logging
from Mongo2.resources import ProfileResource, ProfilesResource  # y ProfilesResource si también está aquí
from pymongo import MongoClient

def get_db():
    client = MongoClient("mongodb://localhost:27017")
    db = client["social_media"]  # Cambia por el nombre correcto
    return db

# Logging Middleware
class LoggingMiddleware:
    async def process_request(self, req, resp):
        logging.info(f"Request: {req.method} {req.uri}")
    async def process_response(self, req, resp, resource, req_succeeded):
        logging.info(f"Response: {resp.status}")

# Instanciar Falcon app
app = falcon.asgi.App(middleware=[LoggingMiddleware()])

# Obtener base de datos
db = get_db()

# Crear recursos
profile_resource = ProfileResource(db)
profiles_resource = ProfilesResource(db)

# Definir rutas
app.add_route("/profiles", profiles_resource)
app.add_route("/profiles/{profile_id}", profile_resource)
app.add_route("/profiles/total_posts", ProfilesResource(db), suffix="total_posts")
app.add_route("/profiles/rank_by_engagement", ProfilesResource(db), suffix="rank_by_engagement")
