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
    print("4. Cargar user_posts.csv")
    print("5. Cargar post_likes.csv")
    print("6. Cargar post_comments.csv")
    print("7. Cargar user_logins.csv")
    print("8. Cargar notifications.csv")
    print("9. Cargar user_followers.csv")
    print("10. Cargar saved_posts.csv")
    print("11. Cargar user_feed.csv")
    print("12. Cargar user_interactions.csv")
    print("13. Salir")
    print("--- CONSULTAS ---")
    print("14. Ver seguidores de un usuario")
    print("15. Ver posts guardados")
    print("16. Ver feed de usuario")
    print("17. Ver notificaciones")
    print("18. Ver top likers")

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

def load_csv_generic(session, csv_path, insert_query, parser_func):
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            session.execute(insert_query, parser_func(row))
    print(f"Datos cargados desde {csv_path}")

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
            path = input("Ruta de user_posts.csv: ")
            query = session.prepare("""
                INSERT INTO social_network.user_posts (user_id, post_id, content, created_at)
                VALUES (?, ?, ?, ?)
            """)
            load_csv_generic(session, path, query, lambda r: (
                uuid.UUID(r["user_id"]), uuid.UUID(r["post_id"]), r["content"], datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))))

        elif option == "5":
            path = input("Ruta de post_likes.csv: ")
            query = session.prepare("""
                INSERT INTO social_network.post_likes (post_id, liked_at, user_id)
                VALUES (?, ?, ?)
            """)
            load_csv_generic(session, path, query, lambda r: (
                uuid.UUID(r["post_id"]), datetime.fromisoformat(r["liked_at"].replace("Z", "+00:00")), uuid.UUID(r["user_id"])))

        elif option == "6":
            path = input("Ruta de post_comments.csv: ")
            query = session.prepare("""
                INSERT INTO social_network.post_comments (post_id, comment_id, user_id, commented_at, comment)
                VALUES (?, ?, ?, ?, ?)
            """)
            load_csv_generic(session, path, query, lambda r: (
                uuid.UUID(r["post_id"]), uuid.UUID(r["comment_id"]), uuid.UUID(r["user_id"]), datetime.fromisoformat(r["commented_at"].replace("Z", "+00:00")), r["comment"]))

        elif option == "7":
            path = input("Ruta de user_logins.csv: ")
            query = session.prepare("""
                INSERT INTO social_network.user_logins (user_id, login_time, device_info)
                VALUES (?, ?, ?)
            """)
            load_csv_generic(session, path, query, lambda r: (
                uuid.UUID(r["user_id"]), datetime.fromisoformat(r["login_time"].replace("Z", "+00:00")), r["device_info"]))

        elif option == "8":
            path = input("Ruta de notifications.csv: ")
            query = session.prepare("""
                INSERT INTO social_network.notifications (user_id, created_at, notification_id, type, message)
                VALUES (?, ?, ?, ?, ?)
            """)
            load_csv_generic(session, path, query, lambda r: (
                uuid.UUID(r["user_id"]), datetime.fromisoformat(r["created_at"].replace("Z", "+00:00")), uuid.UUID(r["notification_id"]), r["type"], r["message"]))

        elif option == "9":
            path = input("Ruta de user_followers.csv: ")
            query = session.prepare("""
                INSERT INTO social_network.user_followers (user_id, follower_id, followed_at)
                VALUES (?, ?, ?)
            """)
            load_csv_generic(session, path, query, lambda r: (
                uuid.UUID(r["user_id"]), uuid.UUID(r["follower_id"]), datetime.fromisoformat(r["followed_at"].replace("Z", "+00:00"))))

        elif option == "10":
            path = input("Ruta de saved_posts.csv: ")
            query = session.prepare("""
                INSERT INTO social_network.saved_posts (user_id, post_id, saved_at)
                VALUES (?, ?, ?)
            """)
            load_csv_generic(session, path, query, lambda r: (
                uuid.UUID(r["user_id"]), uuid.UUID(r["post_id"]), datetime.fromisoformat(r["saved_at"].replace("Z", "+00:00"))))

        elif option == "11":
            path = input("Ruta de user_feed.csv: ")
            query = session.prepare("""
                INSERT INTO social_network.user_feed (user_id, post_id, author_id, content, created_at)
                VALUES (?, ?, ?, ?, ?)
            """)
            load_csv_generic(session, path, query, lambda r: (
                uuid.UUID(r["user_id"]), uuid.UUID(r["post_id"]), uuid.UUID(r["author_id"]), r["content"], datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))))

        elif option == "12":
            path = input("Ruta de user_interactions.csv: ")
            query = session.prepare("""
                INSERT INTO social_network.user_interactions (user_id, interaction_type, related_user_id, interaction_count)
                VALUES (?, ?, ?, ?)
            """)
            load_csv_generic(session, path, query, lambda r: (
                uuid.UUID(r["user_id"]), r["interaction_type"], uuid.UUID(r["related_user_id"]), int(r["interaction_count"])))

        elif option == "13":
            print("Saliendo...")
            break

        elif option == "14":
            consultar_seguidores(session)
        elif option == "15":
            consultar_guardados(session)
        elif option == "16":
            consultar_feed(session)
        elif option == "17":
            consultar_notificaciones(session)
        elif option == "18":
            consultar_top_likers(session)
        else:
            print("Opción inválida.")

def consultar_seguidores(session):
    user_id = input("Ingrese el UUID del usuario: ")
    query = "SELECT follower_id, followed_at FROM social_network.user_followers WHERE user_id = %s"
    for row in session.execute(query, [uuid.UUID(user_id)]):
        print(f"{row.follower_id} desde {row.followed_at}")

def consultar_guardados(session):
    user_id = input("Ingrese el UUID del usuario: ")
    query = "SELECT post_id, saved_at FROM social_network.saved_posts WHERE user_id = %s"
    for row in session.execute(query, [uuid.UUID(user_id)]):
        print(f"Post: {row.post_id} guardado en {row.saved_at}")

def consultar_feed(session):
    user_id = input("Ingrese el UUID del usuario: ")
    query = "SELECT created_at, post_id, author_id, content FROM social_network.user_feed WHERE user_id = %s"
    for row in session.execute(query, [uuid.UUID(user_id)]):
        print(f"{row.created_at} - {row.author_id}: {row.content}")

def consultar_notificaciones(session):
    user_id = input("Ingrese el UUID del usuario: ")
    query = "SELECT created_at, type, message FROM social_network.notifications WHERE user_id = %s"
    for row in session.execute(query, [uuid.UUID(user_id)]):
        print(f"[{row.created_at}] ({row.type.upper()}): {row.message}")

def consultar_top_likers(session):
    user_id = input("Ingrese el UUID del usuario: ")
    query = """
        SELECT related_user_id, interaction_count FROM social_network.user_interactions
        WHERE user_id = %s AND interaction_type = 'top_likers'
    """
    resultados = session.execute(query, [uuid.UUID(user_id)])
    ordenados = sorted(resultados, key=lambda x: x.interaction_count, reverse=True)
    for r in ordenados[:10]:
        print(f"{r.related_user_id} → {r.interaction_count} interacciones")

if __name__ == "__main__":
    main()
