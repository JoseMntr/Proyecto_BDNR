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
    timestamp: datetime @index(datetime) .

    comment_id: string @index(hash) .
    text: string .
    comment_timestamp: datetime @index(datetime) .

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
        topic_name
    }
    """
    return client.alter(pydgraph.Operation(schema=schema))


""" DGRAPH QUERIES FOR SOCIAL MEDIA """


# Follow a user
def follow_user(follower_id, followee_id, client):
    # Obtener los UID de ambos usuarios
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

    # Crear la relación follows
    txn = client.txn()
    try:
        mutation = {"uid": follower["uid"], "follows": [{"uid": followee["uid"]}]}
        txn.mutate(set_obj=mutation)
        txn.commit()
    finally:
        txn.discard()

    return {
        "follower": {"user_id": follower["user_id"], "name": follower["name"]},
        "followee": {"user_id": followee["user_id"], "name": followee["name"]},
    }


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
        txn.mutate(delete_obj=deletion)
        txn.commit()
    finally:
        txn.discard()

    return {
        "unfollower": {"user_id": follower["user_id"], "name": follower["name"]},
        "unfollowed": {"user_id": followee["user_id"], "name": followee["name"]},
    }


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

    return {
        "user": {"user_id": user["user_id"], "name": user["name"]},
        "post": {"post_id": post["post_id"], "content": post["content"]},
    }


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
    timestamp = datetime.utcnow().isoformat()
    comment_id = f"c-{user_id}-{post_id}-{int(datetime.utcnow().timestamp())}"

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
                    "timestamp": timestamp,
                    "commented_on": {"uid": post["uid"]},
                }
            ],
        }
        txn.mutate(set_obj=mutation)
        txn.commit()
    finally:
        txn.discard()

    return {
        "user": {"user_id": user["user_id"], "name": user["name"]},
        "post": {"post_id": post["post_id"], "content": post["content"]},
        "comment": {"comment_id": comment_id, "text": text, "timestamp": timestamp},
    }


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

    return {
        "user": {"user_id": user["user_id"], "name": user["name"]},
        "following": following,
    }


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

    return {
        "user": {"user_id": user["user_id"], "name": user["name"]},
        "followers": followers,
    }


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
          timestamp
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
                "timestamp": comment["timestamp"],
            }
        )

    return {
        "post": {"post_id": post["post_id"], "content": post["content"]},
        "comments": comments,
    }


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

    user1_follows = {u["uid"]: u for u in data.get("user1", [{}])[0].get("follows", [])}
    user2_follows = {u["uid"]: u for u in data.get("user2", [{}])[0].get("follows", [])}

    mutual_uids = set(user1_follows.keys()) & set(user2_follows.keys())
    mutual_users = [user1_follows[uid] for uid in mutual_uids]

    return {"mutual_followees": mutual_users}


# Get recommended content based on user interactions
def recommend_content(user_id, client):
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

    return {
        "user": {"user_id": user_id, "name": user["name"]},
        "recommended_accounts": recommendations,
    }


# Get recommended users based on mutual connections and interests
def recommend_users(user_id, client):
    query_str = f"""
    {{
      user(func: eq(user_id, "{user_id}")) {{
        uid
        name
        follows {{
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
    res = client.txn(read_only=True).query(query_str)
    data = json.loads(res.json)

    user = data.get("user", [None])[0]
    if not user:
        return {"error": "Usuario no encontrado"}

    uid_seen = set()
    score_map = {}

    # Recomendaciones por conexiones mutuas (seguidores de seguidores)
    for followed in user.get("follows", []):
        for suggestion in followed.get("follows", []):
            if suggestion["uid"] != user["uid"]:
                uid = suggestion["uid"]
                if uid not in uid_seen:
                    uid_seen.add(uid)
                    score_map[uid] = {
                        "user_id": suggestion["user_id"],
                        "name": suggestion["name"],
                        "score": 1,
                    }
                else:
                    score_map[uid]["score"] += 1

    # Recomendaciones por intereses comunes
    for topic in user.get("interested_in", []):
        for related_user in topic.get("~interested_in", []):
            if related_user["uid"] != user["uid"]:
                uid = related_user["uid"]
                if uid not in uid_seen:
                    uid_seen.add(uid)
                    score_map[uid] = {
                        "user_id": related_user["user_id"],
                        "name": related_user["name"],
                        "score": 1,
                    }
                else:
                    score_map[uid]["score"] += 1

    # Convertir a lista y ordenar por score
    recommendations = sorted(score_map.values(), key=lambda x: x["score"], reverse=True)

    return {
        "user": {"user_id": user_id, "name": user["name"]},
        "recommended_users": recommendations,
    }


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

    return {
        "user": {"user_id": user_id, "name": user["name"]},
        "recommended_posts": recommendations,
    }


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

    return {
        "user": {"user_id": user_id, "name": user["name"]},
        "recommended_to_share": recommendations,
    }
