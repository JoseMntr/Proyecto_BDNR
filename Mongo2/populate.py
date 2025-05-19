import json
import requests

BASE_URL = "http://localhost:8000"

def main():
    total = 0
    creados = 0
    fallidos = 0

    with open("Mongo2/profile_mongo.json", encoding="utf-8") as fd:
        data = json.load(fd)
        for row in data:
            total += 1
            try:
                # user_id viene como {"$oid": "..."}; necesitas el valor real como str
                if isinstance(row["user_id"], dict) and "$oid" in row["user_id"]:
                    row["user_id"] = row["user_id"]["$oid"]
                else:
                    row["user_id"] = str(row["user_id"])

                # Solo para asegurarse de que los campos numéricos estén bien
                row["number_of_posts"] = int(row["number_of_posts"])
                row["number_of_followers"] = int(row["number_of_followers"])
                row["number_of_follows"] = int(row["number_of_follows"])
                row["log_in_times"] = int(row["log_in_times"])

                response = requests.post(BASE_URL + "/profiles", json=row)

                if response.ok:
                    creados += 1
                else:
                    fallidos += 1
            except Exception:
                fallidos += 1

    print(f"Total de perfiles procesados: {total}")
    print(f"Perfiles creados exitosamente: {creados}")
    print(f"Perfiles con error: {fallidos}")

if __name__ == "__main__":
    main()
