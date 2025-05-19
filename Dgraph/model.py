#!/usr/bin/env python3
import datetime
import json
import csv

import pydgraph


def set_schema(client):
    schema = """
    # ----------- Predicados con índice -----------

    user_id: string @index(hash) .
    name: string .
    email: string @index(hash) .

    post_id: string @index(hash) .
    content: string .  # Tipo de contenido: texto, imagen, video, etc.
    timestamp: string @index(exact) .

    comment_id: string @index(hash) .
    text: string .
    comment_timestamp: string @index(exact) .

    topic_id: string @index(hash) .
    topic_name: string @index(term) .

    follows: [uid] @reverse .
    likes: [uid] @reverse .
    shares: [uid] @reverse .
    comments: [uid] @reverse .
    posts: [uid] @reverse .
    interested_in: [uid] @reverse .
    about: [uid] @reverse .
    commented_on: uid @reverse .

    # ----------- Tipos de nodos -----------

    type User {
        user_id
        name
        email
        follows
        likes
        shares
        comments
        posts
        interested_in
    }

    type Post {
        post_id
        content
        timestamp
        about
    }

    type Comment {
        comment_id
        text
        comment_timestamp
        commented_on
    }

    type Topic {
        topic_id
        topic_name
    }
    """
    return client.alter(pydgraph.Operation(schema=schema))


""" DGRAPH QUERIES FOR SOCIAL MEDIA """


# Follow a user
def follow_user(follower_id, followee_id, client):
    # Obtener los UID de ambos usuarios mediante una consulta
    query_str = f"""
    {{
      follower(func: eq(user_id, "{follower_id}")) {{
        uid
        name
        user_id
      }}
      followee(func: eq(user_id, "{followee_id}")) {{
        uid
        name
        user_id
      }}
    }}
    """
    # Ejecutar la consulta para obtener los datos de los usuarios
    res = client.txn(read_only=True).query(query_str)
    users = json.loads(res.json)

    # Obtener el primer resultado de cada usuario (si existe)
    follower = users.get("follower", [None])[0]
    followee = users.get("followee", [None])[0]

    # Si no se encuentran los usuarios, retornar un mensaje de error
    if not follower or not followee:
        return {"error": "Uno o ambos usuarios no existen"}

    # Crear la relación 'follows' en la base de datos
    txn = client.txn()
    try:
        # Construir el objeto de mutación que define la relación de seguimiento
        mutation = {"uid": follower["uid"], "follows": [{"uid": followee["uid"]}]}
        # Ejecutar la mutación para agregar la relación de seguimiento
        txn.mutate(set_obj=mutation)
        # Confirmar la transacción
        txn.commit()
    finally:
        # Asegurarse de que la transacción se descarte en caso de error
        txn.discard()

    # Crear la respuesta con los datos de los usuarios
    response = {
        "follower": {"user_id": follower["user_id"], "name": follower["name"]},
        "followee": {"user_id": followee["user_id"], "name": followee["name"]},
    }

    # Imprimir la relación de seguimiento creada
    print("\nRelación de seguimiento creada:")
    print(json.dumps(response, indent=2))



# Unfollow a user
def unfollow_user(follower_id, followee_id, client):
    query_str = f"""
    {{
      follower(func: eq(user_id, "{follower_id}")) {{
        uid
        name
        user_id
      }}
      followee(func: eq(user_id, "{followee_id}")) {{
        uid
        name
        user_id
      }}
    }}
    """
    res = client.txn(read_only=True).query(query_str)
    users = json.loads(res.json)

    follower = users.get("follower", [None])[0]
    followee = users.get("followee", [None])[0]

    if not follower or not followee:
        return {"error": "Uno o ambos usuarios no existen"}

    # Borrar la relación follows
    txn = client.txn()
    try:
        deletion = {"uid": follower["uid"], "follows": [{"uid": followee["uid"]}]}
        txn.mutate(del_obj=deletion)
        txn.commit()
    finally:
        txn.discard()

    # Estructura de respuesta
    response = {
        "unfollower": {"user_id": follower["user_id"], "name": follower["name"]},
        "unfollowed": {"user_id": followee["user_id"], "name": followee["name"]},
    }

    print("\nRelación de seguimiento eliminada:")
    print(json.dumps(response, indent=2))


# Like a post
def like_post(user_id, post_id, client):
    # Obtener UIDs del usuario y post
    query_str = f"""
    {{
      user(func: eq(user_id, "{user_id}")) {{
        uid
        name
        user_id
      }}
      post(func: eq(post_id, "{post_id}")) {{
        uid
        content
        post_id
      }}
    }}
    """
    res = client.txn(read_only=True).query(query_str)
    data = json.loads(res.json)

    user = data.get("user", [None])[0]
    post = data.get("post", [None])[0]

    if not user or not post:
        return {"error": "Usuario o post no encontrado"}

    # Crear relación likes
    txn = client.txn()
    try:
        mutation = {"uid": user["uid"], "likes": [{"uid": post["uid"]}]}
        txn.mutate(set_obj=mutation)
        txn.commit()
    finally:
        txn.discard()

    response = {
    "user": {
        "user_id": user["user_id"],
        "name": user["name"]
    },
    "post": {
        "post_id": post["post_id"],
        "content": post["content"]
    }
  }

    print("\nInteracción registrada:")
    print(json.dumps(response, indent=2))


# Comment on a post
def comment_post(user_id, post_id, text, client):
    # Obtener UIDs del usuario y post
    query_str = f"""
    {{
      user(func: eq(user_id, "{user_id}")) {{
        uid
        name
        user_id
      }}
      post(func: eq(post_id, "{post_id}")) {{
        uid
        content
        post_id
      }}
    }}
    """
    res = client.txn(read_only=True).query(query_str)
    data = json.loads(res.json)

    user = data.get("user", [None])[0]
    post = data.get("post", [None])[0]

    if not user or not post:
        return {"error": "Usuario o post no encontrado"}

    # Crear comentario y relaciones
    timestamp = datetime.datetime.now().isoformat()
    print("timestamp", timestamp)
    comment_id = f"c-{user_id}-{post_id}-{int(datetime.datetime.now().timestamp())}"

    txn = client.txn()
    try:
        mutation = {
            "uid": user["uid"],
            "comments": [
                {
                    "uid": "_:new_comment",
                    "dgraph.type": "Comment",
                    "comment_id": comment_id,
                    "text": text,
                    "comment_timestamp": timestamp,
                    "commented_on": {"uid": post["uid"]},
                }
            ],
        }
        txn.mutate(set_obj=mutation)
        txn.commit()
    finally:
        txn.discard()

    response = {
        "user": {"user_id": user["user_id"], "name": user["name"]},
        "post": {"post_id": post["post_id"], "content": post["content"]},
        "comment": {
            "comment_id": comment_id,
            "text": text,
            "comment_timestamp": timestamp,
        },
    }
    print("\nComentario registrado:")
    print(json.dumps(response, indent=2))


# Get followers of a user
def get_following(user_id, client):
    query_str = f"""
    {{
      user(func: eq(user_id, "{user_id}")) {{
        name
        user_id
        follows {{
          user_id
          name
        }}
      }}
    }}
    """
    res = client.txn(read_only=True).query(query_str)
    data = json.loads(res.json)
    user = data.get("user", [None])[0]

    if not user:
        return {"error": "Usuario no encontrado"}

    following = user.get("follows", [])

    response = {
        "user": {"user_id": user["user_id"], "name": user["name"]},
        "following": following,
    }
    print("\nSiguiendo:")
    print(json.dumps(response, indent=2))


# Get followers of a user
def get_followers(user_id, client):
    query_str = f"""
    {{
      user(func: eq(user_id, "{user_id}")) {{
        name
        user_id
        ~follows {{
          user_id
          name
        }}
      }}
    }}
    """
    res = client.txn(read_only=True).query(query_str)
    data = json.loads(res.json)
    user = data.get("user", [None])[0]

    if not user:
        return {"error": "Usuario no encontrado"}

    followers = user.get("~follows", [])

    response = {
        "user": {"user_id": user["user_id"], "name": user["name"]},
        "followers": followers,
    }
    print("\nSeguidores:")
    print(json.dumps(response, indent=2))


# Get comments on a post
def get_comments(post_id, client):
    query_str = f"""
    {{
      post(func: eq(post_id, "{post_id}")) {{
        post_id
        content
        ~commented_on {{
          comment_id
          text
          comment_timestamp
          ~comments {{
            user_id
            name
          }}
        }}
      }}
    }}
    """
    res = client.txn(read_only=True).query(query_str)
    data = json.loads(res.json)
    post = data.get("post", [None])[0]

    if not post:
        return {"error": "Post no encontrado"}

    comments = []
    for comment in post.get("~commented_on", []):
        user = comment.get("~comments", [{}])[0]
        comments.append(
            {
                "user_id": user.get("user_id", "N/A"),
                "name": user.get("name", "N/A"),
                "comment_text": comment["text"],
                "comment_timestamp": comment["comment_timestamp"],
            }
        )

    response = {
        "post": {"post_id": post["post_id"], "content": post["content"]},
        "comments": comments,
    }
    print("\nComentarios en el post:")
    print(json.dumps(response, indent=2))


# Get mutual followers between two users
def get_mutual_followers(user1_id, user2_id, client):
    query_str = f"""
    {{
      user1(func: eq(user_id, "{user1_id}")) {{
        follows {{
          uid
          user_id
          name
        }}
      }}
      user2(func: eq(user_id, "{user2_id}")) {{
        follows {{
          uid
          user_id
          name
        }}
      }}
    }}
    """
    res = client.txn(read_only=True).query(query_str)
    data = json.loads(res.json)

    try:
      user1_follows = {u["uid"]: u for u in data.get("user1", [{}])[0].get("follows", [])}
      user2_follows = {u["uid"]: u for u in data.get("user2", [{}])[0].get("follows", [])}

      mutual_uids = set(user1_follows.keys()) & set(user2_follows.keys())
      
      mutual_users = [user1_follows[uid] for uid in mutual_uids]
    
    except Exception as e:
        print("No se encontraron usuarios")
        return

    response = {"mutual_followees": mutual_users}

    print("\nSeguidores mutuos:")
    print(json.dumps(response, indent=2))


# Get recommended content based on user interactions
def recomend_users_by_interaction(user_id, client):
    query_str = f"""
    {{
      user(func: eq(user_id, "{user_id}")) {{
        name
        likes {{
          ~posts {{
            user_id
            name
          }}
        }}
        comments {{
          ~comments {{
            user_id
            name
          }}
        }}
        shares {{
          ~shares {{
            user_id
            name
          }}
        }}
      }}
    }}
    """
    res = client.txn(read_only=True).query(query_str)
    data = json.loads(res.json)

    user = data.get("user", [None])[0]
    if not user:
        return {"error": "Usuario no encontrado"}

    interaction_counts = {}

    for relation in ["likes", "comments", "shares"]:
        for related in user.get(relation, []):
            target_user = related.get(
                f"~{'posts' if relation == 'likes' else relation}", [{}]
            )[0]
            if "user_id" in target_user:
                uid = target_user["user_id"]
                if uid == user_id:
                    continue  # Ignorar interacciones consigo mismo
                interaction_counts[uid] = interaction_counts.get(
                    uid, {"count": 0, "name": target_user["name"]}
                )
                interaction_counts[uid]["count"] += 1

    # Ordenar por mayor número de interacciones
    sorted_users = sorted(
        interaction_counts.items(), key=lambda x: x[1]["count"], reverse=True
    )

    recommendations = [
        {"user_id": uid, "name": info["name"], "interaction_score": info["count"]}
        for uid, info in sorted_users
    ]

    response = {
        "user": {"user_id": user_id, "name": user["name"]},
        "recommended_accounts": recommendations,
    }
    print("\nCuentas recomendadas por interacción:")
    print(json.dumps(response, indent=2))


# Get recommended users based on mutual connections and interests
# Función para recomendar usuarios a seguir en base a conexiones mutuas e intereses comunes
def recommend_users(user_id, client):
    # Consulta para obtener los usuarios que sigue el usuario dado y sus intereses
    query_str = f"""
    {{
      user(func: eq(user_id, "{user_id}")) {{
        uid
        name
        follows {{
          uid
          user_id
          follows {{
            uid
            user_id
            name
          }}
        }}
        interested_in {{
          ~interested_in {{
            uid
            user_id
            name
          }}
        }}
      }}
    }}
    """
    # Ejecutar la consulta para obtener los datos relacionados con el usuario y sus relaciones
    res = client.txn(read_only=True).query(query_str)
    data = json.loads(res.json)

    # Obtener el usuario principal de la respuesta
    user = data.get("user", [None])[0]
    
    # Si el usuario no existe, retornar un mensaje de error
    if not user:
        return {"error": "Usuario no encontrado"}

    # Obtener el UID del usuario principal y los usuarios que ya sigue
    own_uid = user["uid"]
    already_followed_uids = {f["uid"] for f in user.get("follows", [])}
    uid_seen = set()  # Para evitar recomendaciones repetidas
    score_map = {}  # Mapa de puntuación para los usuarios recomendados

    # Recomendaciones por conexiones mutuas (seguidores de seguidores)
    for followed in user.get("follows", []):  # Para cada usuario seguido por el usuario principal
        for suggestion in followed.get("follows", []):  # Buscar los seguidores de cada usuario seguido
            # Si el usuario sugerido no es el mismo que el principal y no lo sigue ya
            if suggestion["uid"] != own_uid and suggestion["uid"] not in already_followed_uids:
                uid = suggestion["uid"]
                # Si aún no se ha añadido este usuario al conjunto de recomendaciones
                if uid not in uid_seen:
                    uid_seen.add(uid)
                    score_map[uid] = {
                        "user_id": suggestion["user_id"],
                        "name": suggestion["name"],
                        "score": 1,  # Asignar una puntuación inicial de 1
                    }
                else:
                    # Si ya existe, incrementar la puntuación para indicar mayor conexión
                    score_map[uid]["score"] += 1

    # Recomendaciones por intereses comunes
    for topic in user.get("interested_in", []):  # Para cada interés del usuario principal
        for related_user in topic.get("~interested_in", []):  # Buscar usuarios con el mismo interés
            # Asegurarse de que no sea el usuario principal ni un usuario ya seguido
            if related_user["uid"] != own_uid and related_user["uid"] not in already_followed_uids:
                uid = related_user["uid"]
                # Si el usuario aún no está en la lista de recomendaciones
                if uid not in uid_seen:
                    uid_seen.add(uid)
                    score_map[uid] = {
                        "user_id": related_user["user_id"],
                        "name": related_user["name"],
                        "score": 1,  # Asignar puntuación inicial
                    }
                else:
                    # Si ya está en la lista, aumentar su puntuación
                    score_map[uid]["score"] += 1

    # Convertir el mapa de puntuaciones en una lista y ordenarlo de mayor a menor puntuación
    recommendations = sorted(score_map.values(), key=lambda x: x["score"], reverse=True)

    # Crear la respuesta con los usuarios recomendados y el usuario original
    response = {
        "user": {"user_id": user_id, "name": user["name"]},
        "recommended_users": recommendations,
    }

    # Imprimir los usuarios recomendados
    print("\nUsuarios recomendados:")
    print(json.dumps(response, indent=2))




# Get recommended posts based that the user will probably engage with
def recommend_posts(user_id, client):
    query_str = f"""
    {{
      user(func: eq(user_id, "{user_id}")) {{
        uid
        name
        likes {{
          ~posts {{
            uid
            posts {{
              uid
              post_id
              content
            }}
          }}
        }}
        comments {{
          ~comments {{
            uid
            posts {{
              uid
              post_id
              content
            }}
          }}
        }}
        interested_in {{
          ~about {{
            uid
            post_id
            content
          }}
        }}
      }}
    }}
    """
    res = client.txn(read_only=True).query(query_str)
    data = json.loads(res.json)

    user = data.get("user", [None])[0]
    if not user:
        return {"error": "Usuario no encontrado"}

    post_scores = {}

    def add_post_score(post, weight):
        uid = post["uid"]
        if uid not in post_scores:
            post_scores[uid] = {
                "post_id": post.get("post_id", ""),
                "content": post.get("content", ""),
                "score": 0,
            }
        post_scores[uid]["score"] += weight

    # Posts de autores que le gustan/comenta
    for like in user.get("likes", []):
        for author in like.get("~posts", []):
            for post in author.get("posts", []):
                add_post_score(post, 2)

    for comment in user.get("comments", []):
        for author in comment.get("~comments", []):
            for post in author.get("posts", []):
                add_post_score(post, 2)

    # Posts relacionados con intereses (temas)
    for topic in user.get("interested_in", []):
        for post in topic.get("~about", []):
            add_post_score(post, 1)

    recommendations = sorted(
        post_scores.values(), key=lambda x: x["score"], reverse=True
    )

    response = {
        "user": {"user_id": user_id, "name": user["name"]},
        "recommended_posts": recommendations,
    }
    print("\nPosts recomendados:")
    print(json.dumps(response, indent=2))


# Get Recommend Posts that the User Will Probably Share
def recommend_posts_to_share(user_id, client):
    query_str = f"""
    {{
      user(func: eq(user_id, "{user_id}")) {{
        uid
        name
        shares {{
          uid
          post_id
          content
          ~posts {{
            uid
            posts {{
              uid
              post_id
              content
            }}
          }}
          about {{
            uid
            name
            ~about {{
              uid
              post_id
              content
            }}
          }}
        }}
      }}
    }}
    """
    res = client.txn(read_only=True).query(query_str)
    data = json.loads(res.json)

    user = data.get("user", [None])[0]
    if not user:
        return {"error": "Usuario no encontrado"}

    post_scores = {}

    def add_score(post, weight):
        uid = post["uid"]
        if uid not in post_scores:
            post_scores[uid] = {
                "post_id": post.get("post_id", ""),
                "content": post.get("content", ""),
                "score": 0,
            }
        post_scores[uid]["score"] += weight

    for shared in user.get("shares", []):
        for author in shared.get("~posts", []):
            for post in author.get("posts", []):
                add_score(post, 2)
        for topic in shared.get("about", []):
            for post in topic.get("~about", []):
                add_score(post, 1)

    # Ordenar por score y agregar "sharing_probability" (normalizado)
    total_score = sum(p["score"] for p in post_scores.values()) or 1
    recommendations = sorted(
        post_scores.values(), key=lambda x: x["score"], reverse=True
    )

    for r in recommendations:
        r["sharing_probability"] = round(r["score"] / total_score, 2)
        del r["score"]

    response = {
        "user": {"user_id": user_id, "name": user["name"]},
        "recommended_to_share": recommendations,
    }
    print("\nPosts recomendados para compartir:")
    print(json.dumps(response, indent=2))


# Drop all data and schema
def drop_all(client):
    print("All data and schema dropped from the graph.")
    return client.alter(pydgraph.Operation(drop_all=True))
