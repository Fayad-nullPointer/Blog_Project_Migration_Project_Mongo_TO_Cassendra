"""
Cassandra-only Data Access Layer

This module is the final version after MongoDB has been completely removed.
Use this file after successfully completing the migration and cleanup.

To switch to this file:
1. Complete migration and verify data in Cassandra
2. Run: mv data_cassandra_only.py data.py
3. Remove MongoDB dependencies from requirements.txt
"""

import os
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


# Cassandra configuration
CASS_CONTACT_POINTS = os.environ.get('CASS_CONTACT_POINTS', '127.0.0.1').split(',')
CASS_PORT = int(os.environ.get('CASS_PORT', 9042))
CASS_KEYSPACE = os.environ.get('CASS_KEYSPACE', 'blog_data')

# Initialize Cassandra connection
cluster = Cluster(CASS_CONTACT_POINTS, port=CASS_PORT)
session = cluster.connect()

# Ensure keyspace and table exist
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {CASS_KEYSPACE}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
""")
session.set_keyspace(CASS_KEYSPACE)

session.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        id int PRIMARY KEY,
        title text,
        content text,
        author text,
        date timestamp
    )
""")

print(f"[Cassandra] Connected to keyspace: {CASS_KEYSPACE}")


def get_posts(sort_by="date"):
    """
    Retrieve all posts from Cassandra.
    
    Args:
        sort_by: 'date' (default, newest first) or 'title' (alphabetical A-Z)
    
    Returns:
        List of post dictionaries
    """
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
    
    if sort_by == "title":
        return sorted(all_posts, key=lambda x: x.get('title', '').lower())
    else:
        return sorted(
            all_posts,
            key=lambda x: x['Date'] if x['Date'] else '',
            reverse=True
        )


def add_post(new_data):
    """
    Add a new post to Cassandra.
    
    Args:
        new_data: Dictionary containing 'title', 'content', and optionally 'author'
    
    Returns:
        Dictionary representing the created post
    """
    # Get the next ID
    max_id_query = "SELECT MAX(id) as max_id FROM posts"
    result = session.execute(max_id_query)
    row = result.one()
    next_id = (row.max_id + 1) if row and row.max_id is not None else 1
    
    # Set defaults
    current_time = datetime.now()
    
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
            next_id,
            new_data['title'],
            new_data['content'],
            new_data['author'],
            current_time
        )
    )
    
    # Return response
    return {
        'id': next_id,
        'title': new_data['title'],
        'content': new_data['content'],
        'author': new_data['author'],
        'Date': current_time.strftime("%Y-%m-%d %H:%M:%S")
    }


def get_user_post_counts():
    """
    Get post count for each author.
    
    Returns:
        List of dictionaries with 'author' and 'count' keys
    """
    query = "SELECT author FROM posts"
    rows = session.execute(query)
    
    # Count posts per author
    author_counts = {}
    for row in rows:
        author = row.author or "Anonymous"
        author_counts[author] = author_counts.get(author, 0) + 1
    
    return [
        {'author': author, 'count': count}
        for author, count in author_counts.items()
    ]


def get_migration_status():
    """Get Cassandra database status."""
    try:
        result = session.execute("SELECT COUNT(*) FROM posts")
        count = result.one()[0]
        return {
            'phase': 'cassandra_only',
            'mongodb_connected': False,
            'cassandra_connected': True,
            'cassandra_post_count': count
        }
    except Exception as e:
        return {
            'phase': 'cassandra_only',
            'mongodb_connected': False,
            'cassandra_connected': False,
            'error': str(e)
        }
