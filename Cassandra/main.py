from cassandra.cluster import Cluster
from . import model
import uuid
from datetime import datetime
import csv


def main_menu():
    print("\n--- MENÚ CASSANDRA ---")
    print("1. Crear keyspace y tablas")
    print("2. Insertar un post")
    print("3. Ver posts de un usuario")
    print("4. Cargar datos")
    print("5. Regresar al menú principal")
    print("0. Eliminar keyspace")
    print("--- CONSULTAS ---")
    print("6. Ver seguidores de un usuario")                  # 13566838-69cc-46ea-9352-ca754c24e3b1
    print("7. Ver posts guardados")                           # 9bcd61b5-3c07-4933-b7c8-039ea26d47ff
    print("8. Ver feed de usuario")                           # d26b3c89-fa12-47c3-9b26-2fc8355ac1bc
    print("9. Ver notificaciones")                            # ad7d321a-6c00-4097-a9c7-c48d8cb35f04
    print("10. Ver top likers")
    print("11. Verificar si user_id existe en user_posts")
    print("12. Ver comentarios de un post")
    print("13. Ver likes de un post")
    print("14. Ver historial de login de un usuario")
    print("15. Ver posts guardados por un usuario")
    print("16. Ver vistas de un post")

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
            load_data()
            # path = "Cassandra/data/user_posts.csv"
            # query = session.prepare("""
            #     INSERT INTO social_network.user_posts (user_id, post_id, content, created_at)
            #     VALUES (?, ?, ?, ?)
            # """)
            # load_csv_generic(session, path, query, lambda r: (
            #     uuid.UUID(r["user_id"]), uuid.UUID(r["post_id"]), r["content"], datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))))

            # path = "Cassandra/data/post_likes.csv"
            # query = session.prepare("""
            #     INSERT INTO social_network.post_likes (post_id, liked_at, user_id)
            #     VALUES (?, ?, ?)
            # """)
            # load_csv_generic(session, path, query, lambda r: (
            #     uuid.UUID(r["post_id"]), datetime.fromisoformat(r["liked_at"].replace("Z", "+00:00")), uuid.UUID(r["user_id"])))

            # path = "Cassandra/data/post_comments.csv"
            # query = session.prepare("""
            #     INSERT INTO social_network.post_comments (post_id, comment_id, user_id, commented_at, comment)
            #     VALUES (?, ?, ?, ?, ?)
            # """)
            # load_csv_generic(session, path, query, lambda r: (
            #     uuid.UUID(r["post_id"]), uuid.UUID(r["comment_id"]), uuid.UUID(r["user_id"]), datetime.fromisoformat(r["commented_at"].replace("Z", "+00:00")), r["comment"]))

            # path = "Cassandra/data/user_logins.csv"
            # query = session.prepare("""
            #     INSERT INTO social_network.user_logins (user_id, login_time, device_info)
            #     VALUES (?, ?, ?)
            # """)
            # load_csv_generic(session, path, query, lambda r: (
            #     uuid.UUID(r["user_id"]), datetime.fromisoformat(r["login_time"].replace("Z", "+00:00")), r["device_info"]))

            # path = "Cassandra/data/notifications.csv"
            # query = session.prepare("""
            #     INSERT INTO social_network.notifications (user_id, created_at, notification_id, type, message)
            #     VALUES (?, ?, ?, ?, ?)
            # """)
            # load_csv_generic(session, path, query, lambda r: (
            #     uuid.UUID(r["user_id"]), datetime.fromisoformat(r["created_at"].replace("Z", "+00:00")), uuid.UUID(r["notification_id"]), r["type"], r["message"]))

            # path = "Cassandra/data/user_followers.csv"
            # query = session.prepare("""
            #     INSERT INTO social_network.user_followers (user_id, follower_id, followed_at)
            #     VALUES (?, ?, ?)
            # """)
            # load_csv_generic(session, path, query, lambda r: (
            #     uuid.UUID(r["user_id"]), uuid.UUID(r["follower_id"]), datetime.fromisoformat(r["followed_at"].replace("Z", "+00:00"))))

            # path = "Cassandra/data/saved_posts.csv"
            # query = session.prepare("""
            #     INSERT INTO social_network.saved_posts (user_id, post_id, saved_at)
            #     VALUES (?, ?, ?)
            # """)
            # load_csv_generic(session, path, query, lambda r: (
            #     uuid.UUID(r["user_id"]), uuid.UUID(r["post_id"]), datetime.fromisoformat(r["saved_at"].replace("Z", "+00:00"))))

            # path = "Cassandra/data/user_feed.csv"
            # query = session.prepare("""
            #     INSERT INTO social_network.user_feed (user_id, post_id, author_id, content, created_at)
            #     VALUES (?, ?, ?, ?, ?)
            # """)
            # load_csv_generic(session, path, query, lambda r: (
            #     uuid.UUID(r["user_id"]), uuid.UUID(r["post_id"]), uuid.UUID(r["author_id"]), r["content"], datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))))

            # path = "Cassandra/data/user_interactions.csv"
            # query = session.prepare("""
            #     INSERT INTO social_network.user_interactions (user_id, interaction_type, related_user_id, interaction_count)
            #     VALUES (?, ?, ?, ?)
            # """)
            # load_csv_generic(session, path, query, lambda r: (
            #     uuid.UUID(r["user_id"]), r["interaction_type"], uuid.UUID(r["related_user_id"]), int(r["interaction_count"])))

            # path = "Cassandra/data/post_views.csv"
            # query = session.prepare("""
            #     INSERT INTO social_network.post_views (post_id, viewed_at, user_id)
            #     VALUES (?, ?, ?)
            # """)
            # load_csv_generic(session, path, query, lambda r: (
            #     uuid.UUID(r["post_id"]), datetime.fromisoformat(r["viewed_at"].replace("Z", "+00:00")), uuid.UUID(r["user_id"])))

        elif option == "5":
            print("Saliendo...")
            break

        elif option == "6":
            consultar_seguidores(session)
        elif option == "7":
            consultar_guardados(session)
        elif option == "8":
            consultar_feed(session)
        elif option == "9":
            consultar_notificaciones(session)
        elif option == "10":
            consultar_top_likers(session)
        elif option == "11":
            verificar_usuario_en_posts(session)
        elif option == "12":
            ver_comentarios_post(session)
        elif option == "13":
            ver_likes_post(session)
        elif option == "14":
            ver_logins_usuario(session)
        elif option == "15":
            ver_guardados_por_usuario(session)
        elif option == "16":
            ver_vistas_post(session)
        elif option == "0":
            drop_keyspace(session)
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


def load_data():
    session = model.connect_and_initialize()
    path = "Cassandra/data/user_posts.csv"
    query = session.prepare("""
        INSERT INTO social_network.user_posts (user_id, post_id, content, created_at)
        VALUES (?, ?, ?, ?)
    """)
    load_csv_generic(session, path, query, lambda r: (
        uuid.UUID(r["user_id"]), uuid.UUID(r["post_id"]), r["content"], datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))))

    path = "Cassandra/data/post_likes.csv"
    query = session.prepare("""
        INSERT INTO social_network.post_likes (post_id, liked_at, user_id)
        VALUES (?, ?, ?)
    """)
    load_csv_generic(session, path, query, lambda r: (
        uuid.UUID(r["post_id"]), datetime.fromisoformat(r["liked_at"].replace("Z", "+00:00")), uuid.UUID(r["user_id"])))

    path = "Cassandra/data/post_comments.csv"
    query = session.prepare("""
        INSERT INTO social_network.post_comments (post_id, comment_id, user_id, commented_at, comment)
        VALUES (?, ?, ?, ?, ?)
    """)
    load_csv_generic(session, path, query, lambda r: (
        uuid.UUID(r["post_id"]), uuid.UUID(r["comment_id"]), uuid.UUID(r["user_id"]), datetime.fromisoformat(r["commented_at"].replace("Z", "+00:00")), r["comment"]))

    path = "Cassandra/data/user_logins.csv"
    query = session.prepare("""
        INSERT INTO social_network.user_logins (user_id, login_time, device_info)
        VALUES (?, ?, ?)
    """)
    load_csv_generic(session, path, query, lambda r: (
        uuid.UUID(r["user_id"]), datetime.fromisoformat(r["login_time"].replace("Z", "+00:00")), r["device_info"]))

    path = "Cassandra/data/notifications.csv"
    query = session.prepare("""
        INSERT INTO social_network.notifications (user_id, created_at, notification_id, type, message)
        VALUES (?, ?, ?, ?, ?)
    """)
    load_csv_generic(session, path, query, lambda r: (
        uuid.UUID(r["user_id"]), datetime.fromisoformat(r["created_at"].replace("Z", "+00:00")), uuid.UUID(r["notification_id"]), r["type"], r["message"]))

    path = "Cassandra/data/user_followers.csv"
    query = session.prepare("""
        INSERT INTO social_network.user_followers (user_id, follower_id, followed_at)
        VALUES (?, ?, ?)
    """)
    load_csv_generic(session, path, query, lambda r: (
        uuid.UUID(r["user_id"]), uuid.UUID(r["follower_id"]), datetime.fromisoformat(r["followed_at"].replace("Z", "+00:00"))))

    path = "Cassandra/data/saved_posts.csv"
    query = session.prepare("""
        INSERT INTO social_network.saved_posts (user_id, post_id, saved_at)
        VALUES (?, ?, ?)
    """)
    load_csv_generic(session, path, query, lambda r: (
        uuid.UUID(r["user_id"]), uuid.UUID(r["post_id"]), datetime.fromisoformat(r["saved_at"].replace("Z", "+00:00"))))

    path = "Cassandra/data/user_feed.csv"
    query = session.prepare("""
        INSERT INTO social_network.user_feed (user_id, post_id, author_id, content, created_at)
        VALUES (?, ?, ?, ?, ?)
    """)
    load_csv_generic(session, path, query, lambda r: (
        uuid.UUID(r["user_id"]), uuid.UUID(r["post_id"]), uuid.UUID(r["author_id"]), r["content"], datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))))

    path = "Cassandra/data/user_interactions.csv"
    query = session.prepare("""
        INSERT INTO social_network.user_interactions (user_id, interaction_type, related_user_id, interaction_count)
        VALUES (?, ?, ?, ?)
    """)
    load_csv_generic(session, path, query, lambda r: (
        uuid.UUID(r["user_id"]), r["interaction_type"], uuid.UUID(r["related_user_id"]), int(r["interaction_count"])))

    path = "Cassandra/data/post_views.csv"
    query = session.prepare("""
        INSERT INTO social_network.post_views (post_id, viewed_at, user_id)
        VALUES (?, ?, ?)
    """)
    load_csv_generic(session, path, query, lambda r: (
        uuid.UUID(r["post_id"]), datetime.fromisoformat(r["viewed_at"].replace("Z", "+00:00")), uuid.UUID(r["user_id"])))
    

def drop_keyspace(session):
    query = f"DROP KEYSPACE IF EXISTS social_network;"
    session.execute(query)
    print(f"Keyspace social_network eliminado.")


if __name__ == "__main__":
    main()
