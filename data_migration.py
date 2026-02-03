"""
Data Access Layer with Migration Support

This module provides a unified data access layer that supports 4 migration phases:
1. MONGO_ONLY - All reads/writes go to MongoDB only
2. DUAL_WRITE - Writes go to both MongoDB and Cassandra, reads from MongoDB
3. READ_CASSANDRA - Writes to both, reads from Cassandra
4. CASSANDRA_ONLY - All reads/writes go to Cassandra only

Set the MIGRATION_PHASE environment variable to control the phase.
"""

import os
from datetime import datetime
from enum import Enum


class MigrationPhase(Enum):
    MONGO_ONLY = "mongo_only"
    DUAL_WRITE = "dual_write"
    READ_CASSANDRA = "read_cassandra"
    CASSANDRA_ONLY = "cassandra_only"


# Get current migration phase from environment
MIGRATION_PHASE = MigrationPhase(
    os.environ.get('MIGRATION_PHASE', 'mongo_only').lower()
)

print(f"[Migration] Current phase: {MIGRATION_PHASE.value}")


# ============================================================================
# MongoDB Connection
# ============================================================================

mongo_client = None
mongo_db = None
posts_collection = None

if MIGRATION_PHASE != MigrationPhase.CASSANDRA_ONLY:
    from pymongo import MongoClient
    
    MONGODB_URI = os.environ.get('MONGODB_URI', 'mongodb://localhost:27017/')
    MONGO_DB_NAME = os.environ.get('MONGO_DB', 'blog_database')
    
    try:
        mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        mongo_client.server_info()  # Test connection
        mongo_db = mongo_client[MONGO_DB_NAME]
        posts_collection = mongo_db['posts']
        print(f"[MongoDB] Connected to database: {MONGO_DB_NAME}")
    except Exception as e:
        print(f"[MongoDB] Connection failed: {e}")
        if MIGRATION_PHASE == MigrationPhase.MONGO_ONLY:
            raise


# ============================================================================
# Cassandra Connection
# ============================================================================

cassandra_cluster = None
cassandra_session = None

if MIGRATION_PHASE != MigrationPhase.MONGO_ONLY:
    from cassandra.cluster import Cluster
    from cassandra.query import SimpleStatement
    
    CASS_CONTACT_POINTS = os.environ.get('CASS_CONTACT_POINTS', '127.0.0.1').split(',')
    CASS_PORT = int(os.environ.get('CASS_PORT', 9042))
    CASS_KEYSPACE = os.environ.get('CASS_KEYSPACE', 'blog_data')
    
    try:
        cassandra_cluster = Cluster(CASS_CONTACT_POINTS, port=CASS_PORT)
        cassandra_session = cassandra_cluster.connect()
        
        # Ensure keyspace exists
        cassandra_session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {CASS_KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        cassandra_session.set_keyspace(CASS_KEYSPACE)
        
        # Ensure table exists
        cassandra_session.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id int PRIMARY KEY,
                title text,
                content text,
                author text,
                date timestamp
            )
        """)
        
        print(f"[Cassandra] Connected to keyspace: {CASS_KEYSPACE}")
    except Exception as e:
        print(f"[Cassandra] Connection failed: {e}")
        if MIGRATION_PHASE in [MigrationPhase.READ_CASSANDRA, MigrationPhase.CASSANDRA_ONLY]:
            raise


# ============================================================================
# MongoDB Operations
# ============================================================================

def _mongo_get_posts(sort_by="date"):
    """Get posts from MongoDB."""
    all_posts = list(posts_collection.find({}, {'_id': 0}))
    
    # Format dates for JSON response
    for post in all_posts:
        if isinstance(post.get('Date'), datetime):
            post['Date'] = post['Date'].strftime("%Y-%m-%d %H:%M:%S")
    
    if sort_by == "title":
        return sorted(all_posts, key=lambda x: x.get('title', '').lower())
    else:
        return sorted(all_posts, key=lambda x: x.get('Date', ''), reverse=True)


def _mongo_add_post(new_data):
    """Add post to MongoDB."""
    new_data['Date'] = datetime.now()
    last_post = posts_collection.find_one(sort=[("id", -1)])
    new_data['id'] = (last_post['id'] + 1) if last_post else 1
    
    if 'author' not in new_data or not new_data['author']:
        new_data['author'] = "Anonymous"
    
    posts_collection.insert_one(new_data)
    
    response_data = new_data.copy()
    if '_id' in response_data:
        del response_data['_id']
    response_data['Date'] = response_data['Date'].strftime("%Y-%m-%d %H:%M:%S")
    return response_data


def _mongo_get_user_post_counts():
    """Get user post counts from MongoDB."""
    pipeline = [
        {"$group": {"_id": "$author", "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "author": "$_id", "count": 1}}
    ]
    return list(posts_collection.aggregate(pipeline))


def _mongo_get_next_id():
    """Get the next available post ID from MongoDB."""
    last_post = posts_collection.find_one(sort=[("id", -1)])
    return (last_post['id'] + 1) if last_post else 1


# ============================================================================
# Cassandra Operations
# ============================================================================

def _cassandra_get_posts(sort_by="date"):
    """Get posts from Cassandra."""
    query = "SELECT id, title, content, author, date FROM posts"
    rows = cassandra_session.execute(query)
    
    all_posts = []
    for row in rows:
        post = {
            'id': row.id,
            'title': row.title,
            'content': row.content,
            'author': row.author,
            'Date': row.date.strftime("%Y-%m-%d %H:%M:%S") if row.date else ""
        }
        all_posts.append(post)
    
    if sort_by == "title":
        return sorted(all_posts, key=lambda x: x.get('title', '').lower())
    else:
        return sorted(
            all_posts,
            key=lambda x: x['Date'] if x['Date'] else '',
            reverse=True
        )


def _cassandra_add_post(post_data):
    """Add post to Cassandra."""
    insert_query = """
        INSERT INTO posts (id, title, content, author, date)
        VALUES (?, ?, ?, ?, ?)
    """
    prepared = cassandra_session.prepare(insert_query)
    
    # Parse date if it's a string
    post_date = post_data.get('Date')
    if isinstance(post_date, str):
        post_date = datetime.strptime(post_date, "%Y-%m-%d %H:%M:%S")
    elif not isinstance(post_date, datetime):
        post_date = datetime.now()
    
    cassandra_session.execute(
        prepared,
        (
            post_data['id'],
            post_data['title'],
            post_data['content'],
            post_data.get('author', 'Anonymous'),
            post_date
        )
    )
    
    return {
        'id': post_data['id'],
        'title': post_data['title'],
        'content': post_data['content'],
        'author': post_data.get('author', 'Anonymous'),
        'Date': post_date.strftime("%Y-%m-%d %H:%M:%S")
    }


def _cassandra_get_user_post_counts():
    """Get user post counts from Cassandra."""
    query = "SELECT author FROM posts"
    rows = cassandra_session.execute(query)
    
    author_counts = {}
    for row in rows:
        author = row.author or "Anonymous"
        author_counts[author] = author_counts.get(author, 0) + 1
    
    return [
        {'author': author, 'count': count}
        for author, count in author_counts.items()
    ]


def _cassandra_get_next_id():
    """Get the next available post ID from Cassandra."""
    result = cassandra_session.execute("SELECT MAX(id) as max_id FROM posts")
    row = result.one()
    return (row.max_id + 1) if row and row.max_id is not None else 1


# ============================================================================
# Public API - Phase-aware operations
# ============================================================================

def get_posts(sort_by="date"):
    """
    Retrieve all posts, sorted by date or title.
    
    Read source depends on migration phase:
    - MONGO_ONLY, DUAL_WRITE: Read from MongoDB
    - READ_CASSANDRA, CASSANDRA_ONLY: Read from Cassandra
    """
    if MIGRATION_PHASE in [MigrationPhase.MONGO_ONLY, MigrationPhase.DUAL_WRITE]:
        return _mongo_get_posts(sort_by)
    else:
        return _cassandra_get_posts(sort_by)


def add_post(new_data):
    """
    Add a new post to the database.
    
    Write target depends on migration phase:
    - MONGO_ONLY: Write to MongoDB only
    - DUAL_WRITE, READ_CASSANDRA: Write to both MongoDB and Cassandra
    - CASSANDRA_ONLY: Write to Cassandra only
    """
    # Set defaults
    if 'author' not in new_data or not new_data['author']:
        new_data['author'] = "Anonymous"
    
    if MIGRATION_PHASE == MigrationPhase.MONGO_ONLY:
        return _mongo_add_post(new_data)
    
    elif MIGRATION_PHASE == MigrationPhase.CASSANDRA_ONLY:
        # Get next ID from Cassandra
        new_data['id'] = _cassandra_get_next_id()
        new_data['Date'] = datetime.now()
        return _cassandra_add_post(new_data)
    
    else:
        # DUAL_WRITE or READ_CASSANDRA: Write to both
        # First write to MongoDB to get the ID
        mongo_result = _mongo_add_post(new_data.copy())
        
        # Then write to Cassandra with same data
        cassandra_data = {
            'id': mongo_result['id'],
            'title': mongo_result['title'],
            'content': mongo_result['content'],
            'author': mongo_result['author'],
            'Date': mongo_result['Date']
        }
        
        try:
            _cassandra_add_post(cassandra_data)
            print(f"[Dual Write] Post {mongo_result['id']} written to both databases")
        except Exception as e:
            print(f"[Dual Write] Cassandra write failed: {e}")
            # In production, you might want to handle this differently
        
        return mongo_result


def get_user_post_counts():
    """
    Get post count for each author.
    
    Read source depends on migration phase.
    """
    if MIGRATION_PHASE in [MigrationPhase.MONGO_ONLY, MigrationPhase.DUAL_WRITE]:
        return _mongo_get_user_post_counts()
    else:
        return _cassandra_get_user_post_counts()


def get_migration_status():
    """Get current migration phase and database status."""
    status = {
        'phase': MIGRATION_PHASE.value,
        'mongodb_connected': mongo_client is not None,
        'cassandra_connected': cassandra_session is not None,
    }
    
    # Get counts from each database
    if mongo_client is not None:
        try:
            status['mongodb_post_count'] = posts_collection.count_documents({})
        except:
            status['mongodb_post_count'] = 'error'
    
    if cassandra_session is not None:
        try:
            result = cassandra_session.execute("SELECT COUNT(*) FROM posts")
            status['cassandra_post_count'] = result.one()[0]
        except:
            status['cassandra_post_count'] = 'error'
    
    return status
