from model import connect_and_initialize, get_profile_by_id, get_all_profiles, insert_profile, update_profile, delete_profile
from pymongo import MongoClient
from bson import ObjectId

def menu():
    print("\n--- MENÚ DE OPERACIONES MongoDB ---")
    print("1. Consultar un perfil específico")
    print("2. Ver preferencias de un perfil")
    print("3. Consultar perfiles más usados")
    print("4. Ver perfiles generales (por red social)")
    print("5. Agregar nuevo perfil")
    print("6. Modificar datos de un perfil")
    print("7. Eliminar un perfil")
    print("8. Actualizar preferencias")
    print("9. Agregar total de publicaciones por usuario")
    print("10. Rankear perfiles por engagement")
    print("11. Buscar perfiles por red social")
    print("0. Salir")

def cargar_datos_csv():
    import pandas as pd
    from datetime import datetime
    from model import collection  # Asegúrate de haber hecho connect_and_initialize() antes
    from bson import ObjectId

    path = "data/MOCK_DATA.csv"
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        return

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

    try:
        documents = [transform_row(row) for _, row in df.iterrows()]
        collection.insert_many(documents)
        print(f"✅ Se insertaron {len(documents)} documentos en MongoDB desde el CSV.")
    except Exception as e:
        print(f"Error al insertar documentos: {e}")


def input_object_id(prompt):
    try:
        return ObjectId(input(prompt))
    except Exception:
        print("ID inválido.")
        return None

def main():
    connect_and_initialize()
    cargar_datos_csv()
    while True:
        menu()
        opcion = input("Seleccione una opción: ")

        if opcion == "1":
            pid = input_object_id("Ingrese el ID del perfil: ")
            if pid:
                perfil = get_profile_by_id(str(pid))
                if perfil:
                    print(f"\nUsername: {perfil['username']}\nPosts: {perfil['number_of_posts']}\nFollowers: {perfil['number_of_followers']}\nFollows: {perfil['number_of_follows']}\nFoto: {perfil['profile_picture']}\n")
                else:
                    print("Perfil no encontrado.")

        elif opcion == "2":
            pid = input_object_id("ID del perfil para ver preferencias: ")
            if pid:
                perfil = get_profile_by_id(str(pid))
                if perfil and 'preferences' in perfil:
                    print("Preferencias:", perfil['preferences'])
                else:
                    print("No se encontraron preferencias.")

        elif opcion == "3":
            perfiles = sorted(get_all_profiles(), key=lambda p: p.get("log_in_times", 0) + p.get("number_of_posts", 0), reverse=True)
            for p in perfiles[:5]:
                print(f"{p['username']} - logins: {p.get('log_in_times', 0)}, posts: {p.get('number_of_posts', 0)}")

        elif opcion == "4":
            perfiles = get_all_profiles()
            for p in perfiles:
                print(f"{p['username']} en {p['social_media']}")

        elif opcion == "5":
            doc = {
                "user_id": ObjectId(input("User ID: ")),
                "username": input("Username: "),
                "social_media": input("Plataforma: "),
                "number_of_posts": int(input("Posts: ")),
                "number_of_followers": int(input("Followers: ")),
                "number_of_follows": int(input("Follows: ")),
                "profile_picture": input("URL de la foto: "),
                "log_in_times": int(input("Login times: ")),
                "preferences": {
                    "type": input("Tipo preferido (texto, imagen, video): ")
                }
            }
            insert_profile(doc)
            print("Perfil agregado.")

        elif opcion == "6":
            pid = input_object_id("ID del perfil a modificar: ")
            if pid:
                campo = input("Campo a modificar (e.g., number_of_followers): ")
                valor = input("Nuevo valor: ")
                try:
                    valor = int(valor)
                except:
                    pass
                update_profile(str(pid), {campo: valor})
                print("Perfil actualizado.")

        elif opcion == "7":
            pid = input_object_id("ID del perfil a eliminar: ")
            if pid:
                delete_profile(str(pid))
                print("Perfil eliminado.")

        elif opcion == "8":
            pid = input_object_id("ID del perfil para actualizar preferencias: ")
            if pid:
                new_pref = input("Nuevo tipo preferido (texto, imagen, video): ")
                update_profile(str(pid), {"preferences.type": new_pref})
                print("Preferencias actualizadas.")

        elif opcion == "9":
            user = input_object_id("ID de usuario para total de posts: ")
            from model import collection
            pipeline = [
                {"$match": {"user_id": user}},
                {"$group": {"_id": "$user_id", "total_posts": {"$sum": "$number_of_posts"}}}
            ]
            result = list(collection.aggregate(pipeline))
            print("Total de publicaciones:", result[0]["total_posts"] if result else 0)

        elif opcion == "10":
            from model import collection
            pipeline = [
                {"$addFields": {"engagement_score": {"$add": ["$number_of_followers", "$number_of_follows", "$number_of_posts"]}}},
                {"$sort": {"engagement_score": -1}},
                {"$project": {"username": 1, "engagement_score": 1}}
            ]
            for p in collection.aggregate(pipeline):
                print(f"{p['username']}: {p['engagement_score']}")

        elif opcion == "11":
            red = input("Nombre de red social: ")
            perfiles = [p for p in get_all_profiles() if p["social_media"].lower() == red.lower()]
            for p in perfiles:
                print(f"{p['username']} ({p['social_media']})")

        elif opcion == "0":
            print("Saliendo...")
            break

        else:
            print("Opción inválida. Intente nuevamente.")
