#!/usr/bin/env python3
import os

import pydgraph

from . import model, populate

DGRAPH_URI = os.getenv("DGRAPH_URI", "localhost:9080")


def print_menu():
    mm_options = {
        1: "Poblar base de datos",
        2: "Seguir usuario",
        3: "Dejar de seguir usuario",
        4: "Dar like a un post",
        5: "Comentar un post",
        6: "Ver usuarios seguidos",
        7: "Ver seguidores",
        8: "Ver comentarios en un post",
        9: "Ver seguidores mutuos",
        10: "Recomendar cuentas",
        11: "Recomendar cuentas por interacción",
        12: "Recomendar posts para interactuar",
        13: "Recomendar posts para compartir",
        14: "Borrar toda la data & schema",
        15: "Regresar al menú principal",
    }

    print("\n--- MENÚ PRINCIPAL DGRAPH ---")
    for key in sorted(mm_options.keys()):
        print(f"{key}. {mm_options[key]}")


# Conexión directa con el servidor Dgraph usando gRPC.
def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)


# Crea el cliente principal de Dgraph: realizar operaciones de alto nivel como queries, mutaciones y transacciones.
def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


def close_client_stub(client_stub):
    client_stub.close()


def main(option):
    # Inicializar Client Stub y Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)

    # Crear schema
    model.set_schema(client)
    schemaUp = True

    # print_menu()
    # option = int(input("Introduce tu selección: "))
    if option == 1:
        if not schemaUp:
            schemaUp = model.set_schema(client)
        # Cargar datos desde archivos CSV
        users_uids = populate.load_users("Dgraph/data/nodes/users.csv", client)
        posts_uids = populate.load_posts("Dgraph/data/nodes/posts.csv", client)
        topics_uids = populate.load_topics("Dgraph/data/nodes/topics.csv", client)
        comments_uids = populate.load_comments("Dgraph/data/nodes/comments.csv", client)

        # # Crear relaciones después de cargar nodos
        populate.create_generic_edge(client, "Dgraph/data/edges/follows.csv", users_uids, users_uids, "follower_id", "followed_id", "follows")

        populate.create_generic_edge(client, "Dgraph/data/edges/post.csv", users_uids, posts_uids, "user_id", "post_id", "posts")
        populate.create_generic_edge(client, "Dgraph/data/edges/likes.csv", users_uids, posts_uids, "user_id", "post_id", "likes")
        populate.create_generic_edge(client, "Dgraph/data/edges/shares.csv", users_uids, posts_uids, "user_id", "post_id", "shares")

        populate.create_generic_edge(client, "Dgraph/data/edges/comments.csv", users_uids, comments_uids, "user_id", "comment_id", "comments")
        populate.create_generic_edge(client, "Dgraph/data/edges/commented_on.csv", comments_uids, posts_uids, "comment_id", "post_id", "commented_on")

        populate.create_generic_edge(client, "Dgraph/data/edges/interested_in.csv", users_uids, topics_uids, "user_id", "topic_id", "interested_in")
        populate.create_generic_edge(client, "Dgraph/data/edges/about.csv", posts_uids, topics_uids, "post_id", "topic_id", "about")

        print("Data loaded successfully.")
        return

    elif option == 2:
        u1 = input("ID del seguidor: ")
        u2 = input("ID del seguido: ")
        model.follow_user(u1, u2, client)
        return
    elif option == 3:
        u1 = input("ID del que deja de seguir: ")
        u2 = input("ID del usuario a dejar de seguir: ")
        model.unfollow_user(u1, u2, client)
        return
    elif option == 4:
        u = input("ID del usuario: ")
        p = input("ID del post: ")
        model.like_post(u, p, client)
        return
    elif option == 5:
        u = input("ID del usuario: ")
        p = input("ID del post: ")
        text = input("Texto del comentario: ")
        model.comment_post(u, p, text, client)
        return
    elif option == 6:
        u = input("ID del usuario: ")
        model.get_following(u, client)
        return
    elif option == 7:
        u = input("ID del usuario: ")
        model.get_followers(u, client)
        return
    elif option == 8:
        p = input("ID del post: ")
        model.get_comments(p, client)
        return
    elif option == 9:
        u1 = input("ID del primer usuario: ")
        u2 = input("ID del segundo usuario: ")
        model.get_mutual_followers(u1, u2, client)
        return
    elif option == 10:
        u = input("ID del usuario: ")
        model.recommend_users(u, client)
        return
    elif option == 11:
        u = input("ID del usuario: ")
        model.recomend_users_by_interaction(u, client)
        return
    elif option == 12:
        u = input("ID del usuario: ")
        model.recommend_posts(u, client)
        return
    elif option == 13:
        u = input("ID del usuario: ")
        model.recommend_posts_to_share(u, client)
        return
    elif option == 14:
        model.drop_all(client)
        schemaUp = False
    elif option == 15:
        # model.drop_all(client)
        close_client_stub(client_stub)
        # exit(0)
        return
    else:
        print("Opción no válida.")



def load_data():
    # Inicializar Client Stub y Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)

    # Crear schema
    model.set_schema(client)
    schemaUp = True

    if not schemaUp:
        schemaUp = model.set_schema(client)
    
    # Cargar datos desde archivos CSV
    users_uids = populate.load_users("Dgraph/data/nodes/users.csv", client)
    posts_uids = populate.load_posts("Dgraph/data/nodes/posts.csv", client)
    topics_uids = populate.load_topics("Dgraph/data/nodes/topics.csv", client)
    comments_uids = populate.load_comments("Dgraph/data/nodes/comments.csv", client)

    # Crear relaciones después de cargar nodos
    populate.create_generic_edge(client, "Dgraph/data/edges/follows.csv", users_uids, users_uids, "follower_id", "followed_id", "follows")
    populate.create_generic_edge(client, "Dgraph/data/edges/post.csv", users_uids, posts_uids, "user_id", "post_id", "posts")
    populate.create_generic_edge(client, "Dgraph/data/edges/likes.csv", users_uids, posts_uids, "user_id", "post_id", "likes")
    populate.create_generic_edge(client, "Dgraph/data/edges/shares.csv", users_uids, posts_uids, "user_id", "post_id", "shares")
    populate.create_generic_edge(client, "Dgraph/data/edges/comments.csv", users_uids, comments_uids, "user_id", "comment_id", "comments")
    populate.create_generic_edge(client, "Dgraph/data/edges/commented_on.csv", comments_uids, posts_uids, "comment_id", "post_id", "commented_on")
    populate.create_generic_edge(client, "Dgraph/data/edges/interested_in.csv", users_uids, topics_uids, "user_id", "topic_id", "interested_in")
    populate.create_generic_edge(client, "Dgraph/data/edges/about.csv", posts_uids, topics_uids, "post_id", "topic_id", "about")

    print("Data loaded successfully.")
    
    # Cerrar el cliente y el stub
    close_client_stub(client_stub)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Error: {}".format(e))
