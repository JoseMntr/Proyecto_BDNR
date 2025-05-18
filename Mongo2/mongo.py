#!/usr/bin/env python3
import requests
import json
import Mongo2.resources

BASE_URL = "http://localhost:8000"

def menu():
    print("\n==== MENU PRINCIPAL MONGODB ====")
    print("1. Consultar un perfil")
    print("2. Ver preferencias")
    print("3. Ver perfiles más usados")
    print("4. Ver perfiles generales por usuario")
    print("5. Agregar nuevo perfil")
    print("6. Modificar perfil")
    print("7. Eliminar perfil")
    print("8. Actualizar preferencias")
    print("9. Total de posts por usuario")
    print("10. Rankear perfiles por engagement")
    print("11. Buscar por red social")
    print("12. Mostrar el perfil más activo por usuario")
    print("0. Salir")
    return input("Selecciona una opción: ")

def print_json(obj):
    print(json.dumps(obj, indent=2))

def req_1():
    pid = input("ID del perfil: ")
    res = requests.get(f"{BASE_URL}/profiles/{pid}")
    if res.ok:
        profile = res.json()
        keys = ["username", "number_of_posts", "number_of_followers", "number_of_follows", "profile_picture"]
        filtered = {k: profile[k] for k in keys if k in profile}
        print_json(filtered)
    else:
        print(f"❌ Error: {res.status_code}")

def req_2():
    pid = input("ID del perfil: ")
    res = requests.get(f"{BASE_URL}/profiles/{pid}")
    if res.ok:
        prefs = res.json().get("preferences", {})
        print_json(prefs)
    else:
        print(f"❌ Error: {res.status_code}")

def req_3():
    res = requests.get(f"{BASE_URL}/profiles")
    if res.ok:
        sorted_profiles = sorted(res.json(), key=lambda p: (p.get("log_in_times", 0), p.get("number_of_posts", 0)), reverse=True)
        for p in sorted_profiles:
            print(f"{p['username']} - Logins: {p['log_in_times']}, Posts: {p['number_of_posts']}")
    else:
        print(f"❌ Error: {res.status_code}")

def req_4():
    username = input("Introduce el username: ")
    res = requests.get(f"{BASE_URL}/profiles", params={"username": username})
    if res.ok:
        perfiles = res.json()
        if isinstance(perfiles, dict):
            perfiles = [perfiles]
        if not perfiles:
            print("No se encontraron perfiles para ese username.")
        else:
            for p in perfiles:
                print(f"{p['username']} en {p['social_media']}")
    else:
        print(f"❌ Error: {res.status_code}")


def req_5():
    print("Introduce los datos del nuevo perfil:")
    data = {
        "username": input("username: "),
        "social_media": input("social_media: "),
        "number_of_posts": int(input("number_of_posts: ")),
        "number_of_followers": int(input("number_of_followers: ")),
        "number_of_follows": int(input("number_of_follows: ")),
        "profile_picture": input("profile_picture: "),
        "log_in_times": int(input("log_in_times: ")),
        "preferences": {
            "preferred_types": input("Tipos preferidos (comma): ").split(","),
            "notification_settings": {
                "email": input("Notificación por email (true/false): ").lower() == "true",
                "push": input("Notificación push (true/false): ").lower() == "true"
            }
        }
    }
    res = requests.post(f"{BASE_URL}/profiles", json=data)
    print("✅ Perfil creado" if res.ok else f"❌ Error: {res.status_code} {res.text}")


def req_6():
    editable_fields = {
        1: ("username", str),
        2: ("social_media", str),
        3: ("number_of_posts", int),
        4: ("number_of_followers", int),
        5: ("number_of_follows", int),
        6: ("profile_picture", str),
        7: ("log_in_times", int),
        8: ("preferences.notification_settings.email", bool),
        9: ("preferences.notification_settings.push", bool),
    }
    pid = input("ID del perfil: ")
    print("Campos que puedes modificar:")
    for num, (field, typ) in editable_fields.items():
        print(f"{num}. {field}")
    try:
        choice = int(input("Elige el número del campo a modificar: "))
        field, typ = editable_fields[choice]
    except (ValueError, KeyError):
        print("❌ Opción no válida.")
        return
    if typ is bool:
        new_value = input(f"Nuevo valor para {field} (true/false): ").strip().lower()
        if new_value not in ("true", "false"):
            print("❌ Solo puedes ingresar true o false.")
            return
        new_value = new_value == "true"
    else:
        new_value = input(f"Nuevo valor para {field}: ")
        if typ is int:
            try:
                new_value = int(new_value)
            except ValueError:
                print("❌ El valor debe ser un número.")
                return
    # Construye el diccionario json de forma flexible
    if field.startswith("preferences.notification_settings."):
        setting = field.split(".")[-1]
        json_data = {
            "preferences": {
                "notification_settings": {
                    setting: new_value
                }
            }
        }
    else:
        json_data = {field: new_value}
    res = requests.put(f"{BASE_URL}/profiles/{pid}", json=json_data)
    print("✅ Actualizado" if res.ok else f"❌ Error: {res.status_code}")



def req_7():
    pid = input("ID del perfil: ")
    res = requests.delete(f"{BASE_URL}/profiles/{pid}")
    print("✅ Eliminado" if res.ok else f"❌ Error: {res.status_code}")

def req_8():
    pid = input("ID del perfil: ")
    print("Ejemplo: video, image, text")
    types = input("Nuevos tipos preferidos (separados por coma): ").split(",")
    # Limpia espacios y filtra vacíos
    types = [t.strip() for t in types if t.strip()]
    res = requests.put(f"{BASE_URL}/profiles/{pid}", json={
        "preferences": {
            "preferred_types": types
        }
    })
    print("✅ Preferencias actualizadas" if res.ok else f"❌ Error: {res.status_code}")


def req_9():
    username = input("Username: ")
    res = requests.get(f"{BASE_URL}/profiles/total_posts", params={"username": username})
    if res.ok:
        data = res.json()
        print(f"📊 Total de publicaciones de {username}: {data['total_posts']}")
    else:
        print(f"❌ Error: {res.status_code}")

def req_10():
    username = input("Username (deja vacío para todos): ")
    params = {}
    if username.strip():
        params["username"] = username
    res = requests.get(f"{BASE_URL}/profiles/rank_by_engagement", params=params)
    if res.ok:
        perfiles = res.json()
        for p in perfiles:
            print(f"{p['username']} en {p['social_media']} - Engagement: {p['engagement_score']}")
    else:
        print(f"❌ Error: {res.status_code}")


def req_11():
    sm = input("Plataforma (e.g., Instagram): ")
    res = requests.get(f"{BASE_URL}/profiles", params={"social_media": sm})
    if res.ok:
        for p in res.json():
            print(f"{p['username']} en {p['social_media']}")
    else:
        print(f"❌ Error: {res.status_code}")

def req_12():
    username = input("Introduce el username: ")
    res = requests.get(f"{BASE_URL}/profiles", params={"username": username})
    if res.ok:
        perfiles = res.json()
        if not perfiles:
            print("No hay perfiles para ese usuario.")
            return
        # Encuentra el perfil con más publicaciones
        perfil_top = max(perfiles, key=lambda p: p.get("number_of_posts", 0))
        print(f"El perfil más activo de {username} es en {perfil_top['social_media']} con {perfil_top['number_of_posts']} publicaciones.")
    else:
        print(f"❌ Error: {res.status_code}")


def main(option):
    while True:

        if option == "1":
            req_1()
            return
        elif option == "2":
            req_2()
            return
        elif option == "3":
            req_3()
            return
        elif option == "4":
            req_4()
            return
        elif option == "5":
            req_5()
            return
        elif option == "6":
            req_6()
            return
        elif option == "7":
            req_7()
            return
        elif option == "8":
            req_8()
            return
        elif option == "9":
            req_9()
            return
        elif option == "10":
            req_10()
            return
        elif option == "11":
            req_11()
            return
        elif option == "12":
            req_12()
            return
        elif option == "0":
            break
        else:
            print("❌ Opción no válida")


if __name__ == "__main__":
    main()
