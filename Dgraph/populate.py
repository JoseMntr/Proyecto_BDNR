import pydgraph
import csv


# Load users from CSV file
def load_users(file_path, client):
    txn = client.txn()
    resp = None
    try:
        users = []
        with open(file_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                users.append(
                    {
                        "uid": "_:" + row["user_id"],
                        "dgraph.type": "User",
                        "user_id": row["user_id"],
                        "name": row["name"],
                        "email": row["email"],
                    }
                )
            resp = txn.mutate(set_obj=users)
        txn.commit()
        print("Users cargados exitosamente.")
    finally:
        txn.discard()
    return resp.uids


# Load posts from CSV file
def load_posts(file_path, client):
    txn = client.txn()
    resp = None
    try:
        posts = []
        with open(file_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                about_topics = []
                if row.get("about"):
                    about_topics = [
                        {"uid": "_:" + topic.strip()}
                        for topic in row["about"].split("|")
                    ]

                post = {
                    "uid": "_:" + row["post_id"],
                    "dgraph.type": "Post",
                    "post_id": row["post_id"],
                    "content": row["content"],
                    "timestamp": row["timestamp"],
                    "about": about_topics,
                }
                posts.append(post)

            resp = txn.mutate(set_obj=posts)
        txn.commit()
        print("Posts cargados exitosamente.")
    finally:
        txn.discard()
    return resp.uids


# Load topics from CSV file
def load_topics(file_path, client):
    txn = client.txn()
    resp = None
    try:
        topics = []
        with open(file_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                topic_id = row["topic_id"].strip()
                topic_name = row["topic_name"].strip()
                if topic_name:
                    topics.append(
                        {
                            "uid": "_:" + topic_id,
                            "dgraph.type": "Topic",
                            "topic_name": topic_name,
                        }
                    )
            resp = txn.mutate(set_obj=topics)
        txn.commit()
        print("Temas cargados exitosamente.")
    finally:
        txn.discard()
    return resp.uids


# Load comments from CSV file
def load_comments(file_path, client):
    txn = client.txn()
    resp = None
    try:
        comments = []
        with open(file_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                comment = {
                    "uid": "_:" + row["comment_id"],
                    "dgraph.type": "Comment",
                    "comment_id": row["comment_id"],
                    "text": row["text"],
                    "comment_timestamp": row["comment_timestamp"]
                }
                comments.append(comment)

            resp = txn.mutate(set_obj=comments)
        txn.commit()
        print("Comentarios cargados exitosamente.")
    finally:
        txn.discard()
    return resp.uids



# Create edges between nodes
def create_generic_edge(
    client, file_path, source_uids, target_uids, source_field, target_field, predicate
):
    txn = client.txn()
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                source_uid = source_uids[row[source_field]]
                target_uid = target_uids[row[target_field]]
                mutation = {"uid": source_uid, predicate: {"uid": target_uid}}
                txn.mutate(set_obj=mutation)
        txn.commit()
    finally:
        txn.discard()
