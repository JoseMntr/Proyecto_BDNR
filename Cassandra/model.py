from cassandra.cluster import Cluster

KEYSPACE = "social_network"

def create_keyspace(session):
    session.execute(f'''
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
    ''')

def use_keyspace(session):
    session.execute(f"USE {KEYSPACE}")

def create_tables(session):
    session.execute('''
        CREATE TABLE IF NOT EXISTS user_followers (
            user_id UUID,
            follower_id UUID,
            followed_at TIMESTAMP,
            PRIMARY KEY (user_id, follower_id)
        )
    ''')

    session.execute('''
        CREATE TABLE IF NOT EXISTS post_shares (
            post_id UUID,
            user_id UUID,
            shared_at TIMESTAMP,
            PRIMARY KEY (post_id, user_id)
        )
    ''')

    session.execute('''
        CREATE TABLE IF NOT EXISTS posts (
            post_id UUID PRIMARY KEY,
            user_id UUID,
            content TEXT
        )
    ''')

    session.execute('''
        CREATE TABLE IF NOT EXISTS post_likes (
            post_id UUID,
            liked_at TIMESTAMP,
            user_id UUID,
            PRIMARY KEY (post_id, liked_at, user_id)
        ) WITH CLUSTERING ORDER BY (liked_at DESC, user_id ASC)
    ''')

    session.execute('''
        CREATE TABLE IF NOT EXISTS post_comments (
            post_id UUID,
            comment_id TIMEUUID,
            user_id UUID,
            commented_at TIMESTAMP,
            comment TEXT,
            PRIMARY KEY (post_id, comment_id)
        ) WITH CLUSTERING ORDER BY (comment_id ASC)
    ''')

    session.execute('''
        CREATE TABLE IF NOT EXISTS user_interactions (
            user_id UUID,
            interaction_type TEXT,
            related_user_id UUID,
            interaction_count INT,
            PRIMARY KEY (user_id, interaction_type, related_user_id)
        )
    ''')

    session.execute('''
        CREATE TABLE IF NOT EXISTS user_posts (
            post_id UUID,
            user_id UUID,
            content TEXT,
            created_at TIMESTAMP,
            PRIMARY KEY (user_id, created_at, post_id)
        ) WITH CLUSTERING ORDER BY (created_at DESC, post_id ASC)
    ''')

    session.execute('''
        CREATE TABLE IF NOT EXISTS user_feed (
            user_id UUID,
            post_id UUID,
            author_id UUID,
            content TEXT,
            created_at TIMESTAMP,
            PRIMARY KEY (user_id, created_at, post_id)
        ) WITH CLUSTERING ORDER BY (created_at DESC, post_id ASC)
    ''')

    session.execute('''
        CREATE TABLE IF NOT EXISTS user_comments (
            user_id UUID,
            comment_id UUID,
            post_id UUID,
            comment TEXT,
            commented_at TIMESTAMP,
            PRIMARY KEY (user_id, commented_at, comment_id)
        ) WITH CLUSTERING ORDER BY (commented_at DESC, comment_id ASC)
    ''')

    session.execute('''
        CREATE TABLE IF NOT EXISTS saved_posts (
            user_id UUID,
            post_id UUID,
            saved_at TIMESTAMP,
            PRIMARY KEY (user_id, saved_at, post_id)
        ) WITH CLUSTERING ORDER BY (saved_at DESC, post_id ASC)
    ''')

    session.execute('''
        CREATE TABLE IF NOT EXISTS post_views (
            post_id UUID,
            user_id UUID,
            viewed_at TIMESTAMP,
            PRIMARY KEY (post_id, viewed_at, user_id)
        ) WITH CLUSTERING ORDER BY (viewed_at DESC, user_id ASC)
    ''')

    session.execute('''
        CREATE TABLE IF NOT EXISTS user_logins (
            user_id UUID,
            login_time TIMESTAMP,
            device_info TEXT,
            PRIMARY KEY (user_id, login_time)
        ) WITH CLUSTERING ORDER BY (login_time DESC)
    ''')

    session.execute('''
        CREATE TABLE IF NOT EXISTS notifications (
            user_id UUID,
            notification_id UUID,
            type TEXT,
            message TEXT,
            created_at TIMESTAMP,
            PRIMARY KEY (user_id, created_at, notification_id)
        ) WITH CLUSTERING ORDER BY (created_at DESC, notification_id ASC)
    ''')

def connect():
    cluster = Cluster()
    session = cluster.connect()
    return session
