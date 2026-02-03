"""
Blog-specific migration tool from MongoDB to Cassandra.

This script migrates the blog_database posts collection from MongoDB to Cassandra,
preserving the blog post structure with proper data types.

Author: Custom Blog Migration Tool
"""
import argparse
import json
import os
import sys
from datetime import datetime
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement


def mongo_client(uri):
    """Create MongoDB client connection."""
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        # Test connection
        client.server_info()
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        sys.exit(1)


def cassandra_session(contact_points, port=9042):
    """Create Cassandra session connection."""
    try:
        cluster = Cluster(contact_points, port=port)
        session = cluster.connect()
        return session, cluster
    except Exception as e:
        print(f"Error connecting to Cassandra: {e}")
        sys.exit(1)


def ensure_keyspace(session, keyspace, replication_factor=1):
    """Create keyspace if it doesn't exist."""
    cql = f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {replication_factor}}}
    """
    try:
        session.execute(cql)
        print(f"Keyspace '{keyspace}' ready.")
    except Exception as e:
        print(f"Error creating keyspace: {e}")
        sys.exit(1)


def ensure_blog_table(session, keyspace):
    """
    Create the blog posts table with proper schema.
    
    Schema:
    - id: int (primary key) - post ID
    - title: text - post title
    - content: text - post content
    - author: text - post author
    - date: timestamp - post creation date
    """
    cql = f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.posts (
            id int PRIMARY KEY,
            title text,
            content text,
            author text,
            date timestamp
        )
    """
    try:
        session.execute(cql)
        print(f"Table '{keyspace}.posts' ready.")
    except Exception as e:
        print(f"Error creating table: {e}")
        sys.exit(1)


def migrate_blog_posts(mongo_db, cass_session, keyspace, batch_size=50, dry_run=False):
    """
    Migrate blog posts from MongoDB to Cassandra.
    
    Args:
        mongo_db: MongoDB database object
        cass_session: Cassandra session object
        keyspace: Target Cassandra keyspace
        batch_size: Number of records per batch
        dry_run: If True, only show what would be migrated
    
    Returns:
        Number of migrated posts
    """
    posts_collection = mongo_db['posts']
    total = posts_collection.count_documents({})
    
    if total == 0:
        print("No posts found in MongoDB. Nothing to migrate.")
        return 0
    
    print(f"Found {total} posts to migrate.")
    
    # Ensure table exists
    ensure_blog_table(cass_session, keyspace)
    
    # Prepare insert statement
    insert_cql = f"""
        INSERT INTO {keyspace}.posts (id, title, content, author, date)
        VALUES (?, ?, ?, ?, ?)
    """
    prepared = cass_session.prepare(insert_cql)
    
    migrated = 0
    errors = 0
    cursor = posts_collection.find({})
    
    batch = BatchStatement()
    
    for doc in cursor:
        try:
            # Extract fields with defaults
            post_id = doc.get('id')
            title = doc.get('title', 'Untitled')
            content = doc.get('content', '')
            author = doc.get('author', 'Anonymous')
            
            # Handle date field (could be 'Date' or 'date')
            post_date = doc.get('Date') or doc.get('date')
            if isinstance(post_date, str):
                # Parse string date
                try:
                    post_date = datetime.strptime(post_date, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    post_date = datetime.now()
            elif not isinstance(post_date, datetime):
                post_date = datetime.now()
            
            if dry_run:
                print(f"[DRY RUN] Would insert: id={post_id}, title='{title}', author='{author}'")
                migrated += 1
                continue
            
            # Add to batch
            batch.add(prepared, (post_id, title, content, author, post_date))
            
            # Execute batch when it reaches batch_size
            if len(batch) >= batch_size:
                cass_session.execute(batch)
                batch.clear()
                print(f"Progress: {migrated + 1}/{total} posts migrated...")
            
            migrated += 1
            
        except Exception as e:
            errors += 1
            print(f"Error migrating post (id={doc.get('id', 'unknown')}): {e}")
            continue
    
    # Execute remaining batch
    if not dry_run and len(batch) > 0:
        try:
            cass_session.execute(batch)
        except Exception as e:
            print(f"Error executing final batch: {e}")
    
    print(f"\nMigration complete!")
    print(f"Successfully migrated: {migrated}/{total} posts")
    if errors > 0:
        print(f"Errors encountered: {errors}")
    
    return migrated


def verify_migration(cass_session, keyspace, expected_count):
    """Verify that migration was successful by counting records."""
    try:
        count_cql = f"SELECT COUNT(*) FROM {keyspace}.posts"
        result = cass_session.execute(count_cql)
        actual_count = result.one()[0]
        
        print(f"\nVerification:")
        print(f"Expected posts: {expected_count}")
        print(f"Actual posts in Cassandra: {actual_count}")
        
        if actual_count == expected_count:
            print("✓ Migration verified successfully!")
            return True
        else:
            print("✗ Warning: Post count mismatch!")
            return False
    except Exception as e:
        print(f"Error during verification: {e}")
        return False


def parse_args():
    """Parse command line arguments."""
    p = argparse.ArgumentParser(
        description="Migrate blog posts from MongoDB to Cassandra",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Migrate with default settings
  python migrate_blog_to_cassandra.py
  
  # Dry run to see what would be migrated
  python migrate_blog_to_cassandra.py --dry-run
  
  # Custom MongoDB and Cassandra settings
  python migrate_blog_to_cassandra.py --mongodb-uri mongodb://localhost:27017 \\
      --mongo-db blog_database --cass-keyspace blog_data
        """
    )
    
    p.add_argument(
        '--mongodb-uri',
        default=os.environ.get('MONGODB_URI', 'mongodb://localhost:27017'),
        help='MongoDB connection URI (default: mongodb://localhost:27017)'
    )
    p.add_argument(
        '--mongo-db',
        default=os.environ.get('MONGO_DB', 'blog_database'),
        help='MongoDB database name (default: blog_database)'
    )
    p.add_argument(
        '--cass-hosts',
        default=os.environ.get('CASS_CONTACT_POINTS', '127.0.0.1'),
        help='Comma-separated Cassandra contact points (default: 127.0.0.1)'
    )
    p.add_argument(
        '--cass-port',
        type=int,
        default=int(os.environ.get('CASS_PORT', 9042)),
        help='Cassandra port (default: 9042)'
    )
    p.add_argument(
        '--cass-keyspace',
        default=os.environ.get('CASS_KEYSPACE', 'blog_data'),
        help='Cassandra keyspace (default: blog_data)'
    )
    p.add_argument(
        '--batch-size',
        type=int,
        default=50,
        help='Batch size for Cassandra inserts (default: 50)'
    )
    p.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be migrated without writing to Cassandra'
    )
    p.add_argument(
        '--skip-verification',
        action='store_true',
        help='Skip verification step after migration'
    )
    
    return p.parse_args()


def main():
    """Main migration function."""
    print("=" * 60)
    print("Blog MongoDB to Cassandra Migration Tool")
    print("=" * 60)
    print()
    
    args = parse_args()
    
    # Display configuration
    print("Configuration:")
    print(f"  MongoDB URI: {args.mongodb_uri}")
    print(f"  MongoDB Database: {args.mongo_db}")
    print(f"  Cassandra Hosts: {args.cass_hosts}")
    print(f"  Cassandra Port: {args.cass_port}")
    print(f"  Cassandra Keyspace: {args.cass_keyspace}")
    print(f"  Batch Size: {args.batch_size}")
    print(f"  Dry Run: {args.dry_run}")
    print()
    
    # Connect to MongoDB
    print("Connecting to MongoDB...")
    mongo = mongo_client(args.mongodb_uri)
    mongo_db = mongo[args.mongo_db]
    print("✓ Connected to MongoDB")
    
    # Connect to Cassandra
    print("Connecting to Cassandra...")
    cass_hosts = [h.strip() for h in args.cass_hosts.split(',') if h.strip()]
    cass_session, cluster = cassandra_session(cass_hosts, port=args.cass_port)
    print("✓ Connected to Cassandra")
    
    # Ensure keyspace
    print(f"Setting up keyspace '{args.cass_keyspace}'...")
    ensure_keyspace(cass_session, args.cass_keyspace)
    cass_session.set_keyspace(args.cass_keyspace)
    
    # Migrate posts
    print("\nStarting migration...")
    print("-" * 60)
    migrated_count = migrate_blog_posts(
        mongo_db,
        cass_session,
        args.cass_keyspace,
        batch_size=args.batch_size,
        dry_run=args.dry_run
    )
    print("-" * 60)
    
    # Verify migration
    if not args.dry_run and not args.skip_verification and migrated_count > 0:
        verify_migration(cass_session, args.cass_keyspace, migrated_count)
    
    # Cleanup
    cluster.shutdown()
    mongo.close()
    
    print("\n" + "=" * 60)
    print("Migration process completed!")
    print("=" * 60)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nMigration interrupted by user.")
        sys.exit(130)
    except Exception as e:
        print(f"\n✗ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)