from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from bson import ObjectId
from . model  import (
    connect_and_initialize,
    get_all_profiles,
    get_profile_by_id,
    insert_profile,
    update_profile,
    delete_profile
)

app = FastAPI()

# Pydantic model
class ProfileIn(BaseModel):
    user_id: str
    username: str
    social_media: str
    number_of_posts: int
    number_of_followers: int
    number_of_follows: int
    profile_picture: str
    log_in_times: int
    preferences: dict

@app.get("/profiles")
def read_profiles():
    return get_all_profiles()

@app.get("/profiles/{profile_id}")
def read_profile(profile_id: str):
    profile = get_profile_by_id(profile_id)
    if not profile:
        raise HTTPException(status_code=404, detail="Profile not found")
    return profile

@app.post("/profiles")
def create_profile(profile: ProfileIn):
    profile_dict = profile.dict()
    inserted_id = insert_profile(profile_dict)
    return {"inserted_id": str(inserted_id)}

@app.put("/profiles/{profile_id}")
def edit_profile(profile_id: str, profile: ProfileIn):
    result = update_profile(profile_id, profile.dict())
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Profile not found")
    return {"updated": True}

@app.delete("/profiles/{profile_id}")
def remove_profile(profile_id: str):
    result = delete_profile(profile_id)
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Profile not found")
    return {"deleted": True}

def main():
    connect_and_initialize()
    print("Perfiles disponibles:")
    if not get_all_profiles():
        print("No hay perfiles disponibles.")
    else:
        for profile in get_all_profiles():
            print(profile["username"])

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Error: {}".format(e))