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
    print("13. Cargar post_views.csv")
    print("14. Salir")
    print("--- CONSULTAS ---")
    print("15. Ver seguidores de un usuario")                  # 13566838-69cc-46ea-9352-ca754c24e3b1
    print("16. Ver posts guardados")                           # 9bcd61b5-3c07-4933-b7c8-039ea26d47ff
    print("17. Ver feed de usuario")                           # d26b3c89-fa12-47c3-9b26-2fc8355ac1bc
    print("18. Ver notificaciones")                            # ad7d321a-6c00-4097-a9c7-c48d8cb35f04
    print("19. Ver top likers")
    print("20. Verificar si user_id existe en user_posts")
    print("21. Ver comentarios de un post")
    print("22. Ver likes de un post")
    print("23. Ver historial de login de un usuario")
    print("24. Ver posts guardados por un usuario")
    print("25. Ver vistas de un post")

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
            path = input("Ruta de post_views.csv: ")
            query = session.prepare("""
                INSERT INTO social_network.post_views (post_id, viewed_at, user_id)
                VALUES (?, ?, ?)
            """)
            load_csv_generic(session, path, query, lambda r: (
                uuid.UUID(r["post_id"]), datetime.fromisoformat(r["viewed_at"].replace("Z", "+00:00")), uuid.UUID(r["user_id"])))

        elif option == "14":
            print("Saliendo...")
            break

        elif option == "15":
            consultar_seguidores(session)
        elif option == "16":
            consultar_guardados(session)
        elif option == "17":
            consultar_feed(session)
        elif option == "18":
            consultar_notificaciones(session)
        elif option == "19":
            consultar_top_likers(session)
        elif option == "20":
            verificar_usuario_en_posts(session)
        elif option == "21":
            ver_comentarios_post(session)
        elif option == "22":
            ver_likes_post(session)
        elif option == "23":
            ver_logins_usuario(session)
        elif option == "24":
            ver_guardados_por_usuario(session)
        elif option == "25":
            ver_vistas_post(session)
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

# Prueba
def verificar_usuario_en_posts(session):
    user_id = input("Ingresa el UUID del usuario a verificar: ")
    query = """
    SELECT post_id, created_at, content FROM social_network.user_posts
    WHERE user_id = %s
    """
    try:
        results = session.execute(query, [uuid.UUID(user_id)])
        rows = list(results)
        if rows:
            print(f"Usuario {user_id} tiene {len(rows)} post(s):")
            for row in rows:
                print(f"- {row.created_at}: {row.content}")
        else:
            print("Este usuario no tiene posts registrados.")
    except Exception as e:
        print("Error durante la consulta:", e)

#Otras con
def ver_comentarios_post(session):
    post_id = input("Ingrese el UUID del post: ")
    query = """
    SELECT comment_id, user_id, commented_at, comment FROM social_network.post_comments
    WHERE post_id = %s
    """
    for row in session.execute(query, [uuid.UUID(post_id)]):
        print(f"{row.commented_at} - {row.user_id}: {row.comment}")

def ver_likes_post(session):
    post_id = input("Ingrese el UUID del post: ")
    query = """
    SELECT liked_at, user_id FROM social_network.post_likes
    WHERE post_id = %s
    """
    for row in session.execute(query, [uuid.UUID(post_id)]):
        print(f"{row.liked_at} - Liked by: {row.user_id}")

def ver_logins_usuario(session):
    user_id = input("Ingrese el UUID del usuario: ")
    query = """
    SELECT login_time, device_info FROM social_network.user_logins
    WHERE user_id = %s
    """
    for row in session.execute(query, [uuid.UUID(user_id)]):
        print(f"{row.login_time} - Device: {row.device_info}")

def ver_guardados_por_usuario(session):
    user_id = input("Ingrese el UUID del usuario: ")
    query = """
    SELECT post_id, saved_at FROM social_network.saved_posts
    WHERE user_id = %s
    """
    resultados = session.execute(query, [uuid.UUID(user_id)])
    ordenados = sorted(resultados, key=lambda x: x.saved_at, reverse=True)
    for row in ordenados:
        print(f"{row.saved_at} - {row.post_id}")

def ver_vistas_post(session):
    post_id = input("Ingrese el UUID del post: ")
    query = """
    SELECT viewed_at, user_id FROM social_network.post_views
    WHERE post_id = %s
    """
    for row in session.execute(query, [uuid.UUID(post_id)]):
        print(f"{row.viewed_at} - Visto por: {row.user_id}")

if __name__ == "__main__":
    main()
