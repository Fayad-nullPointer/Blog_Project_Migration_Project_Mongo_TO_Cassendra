"""
Data access layer for the blog application.
Can work with both MongoDB and Cassandra backends.
"""
from datetime import datetime
import os

# Determine which database to use
USE_CASSANDRA = os.environ.get('USE_CASSANDRA', 'false').lower() == 'true'

if USE_CASSANDRA:
    # Cassandra imports
    from cassandra.cluster import Cluster
    from cassandra.query import SimpleStatement
    
    # Cassandra configuration
    CASS_CONTACT_POINTS = os.environ.get('CASS_CONTACT_POINTS', '127.0.0.1').split(',')
    CASS_PORT = int(os.environ.get('CASS_PORT', 9042))
    CASS_KEYSPACE = os.environ.get('CASS_KEYSPACE', 'blog_data')
    
    # Initialize Cassandra connection
    cluster = Cluster(CASS_CONTACT_POINTS, port=CASS_PORT)
    session = cluster.connect(CASS_KEYSPACE)
    
    print(f"Using Cassandra backend (keyspace: {CASS_KEYSPACE})")
    
else:
    # MongoDB imports
    from pymongo import MongoClient
    
    # MongoDB configuration
    MONGODB_URI = os.environ.get('MONGODB_URI', 'mongodb://localhost:27017/')
    MONGO_DB_NAME = os.environ.get('MONGO_DB', 'blog_database')
    
    # Initialize MongoDB connection
    client = MongoClient(MONGODB_URI)
    db = client[MONGO_DB_NAME]
    posts_collection = db['posts']
    
    print(f"Using MongoDB backend (database: {MONGO_DB_NAME})")


def get_posts(sort_by="date"):
    """
    Retrieve all posts, sorted by date (newest first) or title (A-Z).
    
    Args:
        sort_by: 'date' for date sorting, 'title' for alphabetical sorting
    
    Returns:
        List of post dictionaries
    """
    if USE_CASSANDRA:
        return _get_posts_cassandra(sort_by)
    else:
        return _get_posts_mongo(sort_by)


def add_post(new_data):
    """
    Add a new post to the database.
    
    Args:
        new_data: Dictionary containing 'title', 'content', and optionally 'author'
    
    Returns:
        Dictionary representing the created post
    """
    if USE_CASSANDRA:
        return _add_post_cassandra(new_data)
    else:
        return _add_post_mongo(new_data)


def get_user_post_counts():
    """
    Get post count for each author.
    
    Returns:
        List of dictionaries with 'author' and 'count' keys
    """
    if USE_CASSANDRA:
        return _get_user_post_counts_cassandra()
    else:
        return _get_user_post_counts_mongo()


# ============================================================================
# MongoDB Implementation
# ============================================================================

def _get_posts_mongo(sort_by="date"):
    """Get posts from MongoDB."""
    all_posts = list(posts_collection.find({}, {'_id': 0}))
    
    if sort_by == "title":
        return sorted(all_posts, key=lambda x: x.get('title', '').lower())
    else:
        return sorted(all_posts, key=lambda x: x.get('Date', datetime.min), reverse=True)


def _add_post_mongo(new_data):
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


def _get_user_post_counts_mongo():
    """Get user post counts from MongoDB."""
    pipeline = [
        {"$group": {"_id": "$author", "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "author": "$_id", "count": 1}}
    ]
    return list(posts_collection.aggregate(pipeline))


# ============================================================================
# Cassandra Implementation
# ============================================================================

def _get_posts_cassandra(sort_by="date"):
    """Get posts from Cassandra."""
    query = "SELECT id, title, content, author, date FROM posts"
    rows = session.execute(query)
    
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
    
    # Sort posts
    if sort_by == "title":
        return sorted(all_posts, key=lambda x: x.get('title', '').lower())
    else:
        # Sort by date (parse back to datetime for sorting)
        return sorted(
            all_posts,
            key=lambda x: datetime.strptime(x['Date'], "%Y-%m-%d %H:%M:%S") if x['Date'] else datetime.min,
            reverse=True
        )


def _add_post_cassandra(new_data):
    """Add post to Cassandra."""
    # Get the next ID
    max_id_query = "SELECT MAX(id) as max_id FROM posts"
    result = session.execute(max_id_query)
    row = result.one()
    next_id = (row.max_id + 1) if row and row.max_id is not None else 1
    
    # Set defaults
    current_time = datetime.now()
    new_data['id'] = next_id
    new_data['Date'] = current_time
    
    if 'author' not in new_data or not new_data['author']:
        new_data['author'] = "Anonymous"
    
    # Insert into Cassandra
    insert_query = """
        INSERT INTO posts (id, title, content, author, date)
        VALUES (?, ?, ?, ?, ?)
    """
    prepared = session.prepare(insert_query)
    session.execute(
        prepared,
        (
            new_data['id'],
            new_data['title'],
            new_data['content'],
            new_data['author'],
            current_time
        )
    )
    
    # Return response
    response_data = {
        'id': new_data['id'],
        'title': new_data['title'],
        'content': new_data['content'],
        'author': new_data['author'],
        'Date': current_time.strftime("%Y-%m-%d %H:%M:%S")
    }
    return response_data


def _get_user_post_counts_cassandra():
    """Get user post counts from Cassandra."""
    # Cassandra doesn't have aggregation like MongoDB, so we need to do it in Python
    query = "SELECT author FROM posts"
    rows = session.execute(query)
    
    # Count posts per author
    author_counts = {}
    for row in rows:
        author = row.author or "Anonymous"
        author_counts[author] = author_counts.get(author, 0) + 1
    
    # Convert to list format
    return [
        {'author': author, 'count': count}
        for author, count in author_counts.items()
    ]