from cassandra.cluster import Cluster
from . import model
import uuid
from datetime import datetime
import csv
from collections import Counter


def main_menu():
    print("\n--- MENÚ CASSANDRA ---")
    print("1. Crear keyspace y tablas")
    print("2. Insertar un post")
    print("3. Ver posts de un usuario")
    print("4. Cargar datos")
    print("5. Regresar al menú principal")
    print("0. Eliminar keyspace")
    print("--- CONSULTAS ---")
    print("6. Ver seguidores de un usuario")                  
    print("7. Ver posts guardados")                           
    print("8. Ver feed de usuario")                           
    print("9. Ver notificaciones")                            
    print("10. Ver top likers")
    print("11. Verificar si user_id existe en user_posts")
    print("12. Ver comentarios de un post")
    print("13. Ver likes de un post")
    print("14. Ver historial de login de un usuario")
    print("15. Ver posts guardados por un usuario")
    print("16. Ver vistas de un post")
    print("17. Ver top 5 usuarios con más publicaciones")

def insert_post(session):
    # user_id = uuid.uuid4()
    user_id = uuid.UUID(input("Ingresa el UUID del usuario: "))
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

def main(option):
    session = model.connect_and_initialize()

                        
    if option == "1":
        print("Keyspace y tablas ya creadas al iniciar.")
        return
    elif option == "2":
        insert_post(session)
        return
    elif option == "3":
        get_user_posts(session)
        return
    elif option == "4":
        load_data()
        return
    elif option == "5":
        drop_keyspace(session)
        return
    elif option == "6":
        consultar_seguidores(session)
        return
    elif option == "7":
        consultar_guardados(session)
        return
    elif option == "8":
        consultar_feed(session)
        return
    elif option == "9":
        consultar_notificaciones(session)
        return
    elif option == "10":
        consultar_top_likers(session)
        return
    elif option == "11":
        verificar_usuario_en_posts(session)
        return
    elif option == "12":
        ver_comentarios_post(session)
        return
    elif option == "13":
        ver_likes_post(session)
        return
    elif option == "14":
        ver_logins_usuario(session)
        return
    elif option == "15":
        ver_guardados_por_usuario(session)
        return
    elif option == "16":
        ver_vistas_post(session)
        return
    elif option == "17":
        top_usuarios_mas_activos(session)
        return
    
    else:
        print("Opción inválida.")

def consultar_seguidores(session):
    user_id = input("Ingrese el UUID del usuario: ")
    query = "SELECT user_name, followed_at FROM social_network.user_followers WHERE user_id = %s"
    for row in session.execute(query, [uuid.UUID(user_id)]):
        print(f"{row.user_name} desde {row.followed_at}")

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
    SELECT comment_id, user_name, commented_at, comment FROM social_network.post_comments
    WHERE post_id = %s
    """
    for row in session.execute(query, [uuid.UUID(post_id)]):
        print(f"{row.commented_at} - {row.user_name}: {row.comment}")

def ver_likes_post(session):
    post_id = input("Ingrese el UUID del post: ")
    query = """
    SELECT liked_at, user_name FROM social_network.post_likes
    WHERE post_id = %s
    """
    for row in session.execute(query, [uuid.UUID(post_id)]):
        print(f"{row.liked_at} - Liked by: {row.user_name}")

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
    SELECT post_id, user_name FROM social_network.saved_posts
    WHERE user_id = %s
    """
    # resultados = session.execute(query, [uuid.UUID(user_id)])
    
    for row in session.execute(query, [uuid.UUID(user_id)]):
        print(f"{row.user_name} - {row.post_id}")

def ver_vistas_post(session):
    post_id = input("Ingrese el UUID del post: ")
    query = """
    SELECT viewed_at, user_name FROM social_network.post_views
    WHERE post_id = %s
    """
    for row in session.execute(query, [uuid.UUID(post_id)]):
        print(f"{row.viewed_at} - Visto por: {row.user_name}")

def top_usuarios_mas_activos(session):
    query = "SELECT user_id FROM social_network.user_posts"
    user_counter = Counter()

    for row in session.execute(query):
        user_counter[row.user_id] += 1

    print("\nTop 5 usuarios con más publicaciones:")
    for user_id, count in user_counter.most_common(5):
        print(f"Usuario: {user_id} - Posts: {count}")


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
        INSERT INTO social_network.post_likes (post_id, liked_at, user_id, user_name)
        VALUES (?, ?, ?, ?)
    """)
    load_csv_generic(session, path, query, lambda r: (
        uuid.UUID(r["post_id"]), datetime.fromisoformat(r["liked_at"].replace("Z", "+00:00")), uuid.UUID(r["user_id"]), r["user_name"]))

    path = "Cassandra/data/post_comments.csv"
    query = session.prepare("""
        INSERT INTO social_network.post_comments (post_id, comment_id, user_id, commented_at, comment, user_name)
        VALUES (?, ?, ?, ?, ?, ?)
    """)
    load_csv_generic(session, path, query, lambda r: (
        uuid.UUID(r["post_id"]), uuid.UUID(r["comment_id"]), uuid.UUID(r["user_id"]), datetime.fromisoformat(r["commented_at"].replace("Z", "+00:00")), r["comment"], r["user_name"]))

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
        INSERT INTO social_network.user_followers (user_id, follower_id, followed_at, user_name)
        VALUES (?, ?, ?, ?)
    """)
    load_csv_generic(session, path, query, lambda r: (
        uuid.UUID(r["user_id"]), uuid.UUID(r["follower_id"]), datetime.fromisoformat(r["followed_at"].replace("Z", "+00:00")), r["user_name"]))

    path = "Cassandra/data/saved_posts.csv"
    query = session.prepare("""
        INSERT INTO social_network.saved_posts (user_id, post_id, saved_at, user_name)
        VALUES (?, ?, ?, ?)
    """)
    load_csv_generic(session, path, query, lambda r: (
        uuid.UUID(r["user_id"]), uuid.UUID(r["post_id"]), datetime.fromisoformat(r["saved_at"].replace("Z", "+00:00")), r["user_name"]))

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
        INSERT INTO social_network.post_views (post_id, viewed_at, user_id, user_name)
        VALUES (?, ?, ?, ?)
    """)
    load_csv_generic(session, path, query, lambda r: (
        uuid.UUID(r["post_id"]), datetime.fromisoformat(r["viewed_at"].replace("Z", "+00:00")), uuid.UUID(r["user_id"]), r["user_name"]))
    

def drop_keyspace(session):
    query = f"DROP KEYSPACE IF EXISTS social_network;"
    session.execute(query)
    print(f"Keyspace social_network eliminado.")


if __name__ == "__main__":
    main()
