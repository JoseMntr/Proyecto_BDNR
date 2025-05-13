from cassandra.cluster import Cluster
import model
import uuid
from datetime import datetime
import csv

def main_menu():
    print("\n--- MENÚ CASSANDRA ---")
    print("1. Crear keyspace y tablas")
    print("2. Insertar un post")
    print("3. Ver posts de un usuario")
    print("4. Salir")
    print("5. Cargar post_likes desde CSV")

def insert_post(session):
    user_id = uuid.uuid4()
    post_id = uuid.uuid4()
    content = input("Contenido del post: ")
    created_at = datetime.utcnow()

    query = """
    INSERT INTO user_posts (user_id, created_at, post_id, content)
    VALUES (%s, %s, %s, %s)
    """
    session.execute(query, (user_id, created_at, post_id, content))
    print(f"Post agregado con ID: {post_id} y User ID: {user_id}")

def get_user_posts(session):
    user_id = input("Ingresa el UUID del usuario: ")
    query = """
    SELECT post_id, created_at, content FROM user_posts
    WHERE user_id = %s
    """
    try:
        for row in session.execute(query, [uuid.UUID(user_id)]):
            print(f"{row.created_at} - {row.post_id}: {row.content}")
    except Exception as e:
        print("Error en la consulta:", e)


def load_post_likes_from_csv(session, csv_path):
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        insert_query = session.prepare("""
            INSERT INTO social_network.post_likes (post_id, liked_at, user_id)
            VALUES (?, ?, ?)
        """)
        for row in reader:
            post_id = uuid.UUID(row["post_id"])
            user_id = uuid.UUID(row["user_id"])
            liked_at = datetime.fromisoformat(row["liked_at"].replace("Z", "+00:00"))

            session.execute(insert_query, (post_id, liked_at, user_id))

    print("Datos de post_likes cargados correctamente.")

def main():
    session = model.connect_and_initialize()

    while True:
        main_menu()
        option = input("Selecciona una opción: ")

        if option == "1":
            print("Keyspace y tablas ya creadas al iniciar.")
        elif option == "2":
            insert_post(session)
        elif option == "3":
            get_user_posts(session)
        elif option == "4":
            print("Saliendo...")
        elif option == "5":
            csv_path = input("Ruta al archivo post_likes.csv: ")
            load_post_likes_from_csv(session, csv_path)
            break
        else:
            print("Opción inválida.")

if __name__ == "__main__":
    main()