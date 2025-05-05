#!/usr/bin/env python3
import os

import pydgraph

import model

import populate

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')

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
        11: "Recomendar contenido",
        12: "Recomendar posts para interactuar",
        13: "Recomendar posts para compartir",
        0: "Salir"
    }

    print("\n--- MENÚ PRINCIPAL ---")
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

def main():
    # Inicializar Client Stub y Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)

    # Crear schema
    model.set_schema(client)
    schemaUp = True

    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 1:
            if not schemaUp:
                schemaUp = model.set_schema(client)
            # Cargar datos desde archivos CSV
            users_uids = populate.load_users("data/nodes/users.csv", client)
            posts_uids = populate.load_posts("data/nodes/posts.csv", client)
            topics_uids = populate.load_topics("data/nodes/topics.csv", client)
            comments_uids = populate.load_comments("data/nodes/comments.csv", client)

            # Crear relaciones después de cargar nodos

            populate.create_generic_edge(client, "data/edges/follows.csv", users_uids, users_uids, "follower_id", "followed_id", "follows")

            populate.create_generic_edge(client, "data/edges/post.csv", users_uids, posts_uids, "user_id", "post_id", "post")
            populate.create_generic_edge(client, "data/edges/likes.csv", users_uids, posts_uids, "user_id", "post_id", "likes")
            populate.create_generic_edge(client, "data/edges/shares.csv", users_uids, posts_uids, "user_id", "post_id", "shares")
        
            populate.create_generic_edge(client, "data/edges/comments.csv", users_uids, comments_uids, "user_id", "comment_id", "comments")
            populate.create_generic_edge(client, "data/edges/commented_on.csv", comments_uids, posts_uids, "comment_id", "post_id", "commented_on")

            populate.create_generic_edge(client, "data/edges/interested_in.csv", users_uids, topics_uids, "user_id", "topic_id", "interested_in")
            populate.create_generic_edge(client, "data/edges/about.csv", posts_uids, topics_uids, "post_id", "topic_id", "about")
            
            print("Data loaded successfully.")

        elif option == 2:
            u1 = input("ID del seguidor: ")
            u2 = input("ID del seguido: ")
            print(model.follow_user(u1, u2, client))
        elif option == 3:
            u1 = input("ID del que deja de seguir: ")
            u2 = input("ID del usuario a dejar de seguir: ")
            print(model.unfollow_user(u1, u2, client))
        elif option == 4:
            u = input("ID del usuario: ")
            p = input("ID del post: ")
            print(model.like_post(u, p, client))
        elif option == 5:
            u = input("ID del usuario: ")
            p = input("ID del post: ")
            text = input("Texto del comentario: ")
            print(model.comment_post(u, p, text, client))
        elif option == 6:
            u = input("ID del usuario: ")
            print(model.get_following(u, client))
        elif option == 7:
            u = input("ID del usuario: ")
            print(model.get_followers(u, client))
        elif option == 8:
            p = input("ID del post: ")
            print(model.get_comments(p, client))
        elif option == 9:
            u1 = input("ID del primer usuario: ")
            u2 = input("ID del segundo usuario: ")
            print(model.get_mutual_followers(u1, u2, client))
        elif option == 10:
            u = input("ID del usuario: ")
            print(model.recommend_users(u, client))
        elif option == 11:
            u = input("ID del usuario: ")
            print(model.recommend_content(u, client))
        elif option == 12:
            u = input("ID del usuario: ")
            print(model.recommend_posts(u, client))
        elif option == 12:
            u = input("ID del usuario: ")
            print(model.recommend_posts_to_share(u, client))
        elif option == 0:
            print("Saliendo del sistema...")
        else:
            print("Opción no válida.")
if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error: {}'.format(e))