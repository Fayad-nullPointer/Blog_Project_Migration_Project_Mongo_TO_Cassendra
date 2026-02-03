#!/usr/bin/env python3
"""
Migration Controller Script

This script helps manage the MongoDB to Cassandra migration process.
It provides commands to:
1. Check current migration status
2. Run the initial data migration
3. Verify data integrity between databases
4. Switch migration phases
5. Perform cleanup after migration

Usage:
    python migration_controller.py status
    python migration_controller.py migrate
    python migration_controller.py verify
    python migration_controller.py set-phase <phase>
    python migration_controller.py cleanup
"""

import argparse
import os
import sys
from datetime import datetime


def get_mongo_connection():
    """Get MongoDB connection."""
    from pymongo import MongoClient
    
    uri = os.environ.get('MONGODB_URI', 'mongodb://localhost:27017/')
    db_name = os.environ.get('MONGO_DB', 'blog_database')
    
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.server_info()  # Test connection
    return client, client[db_name]


def get_cassandra_connection():
    """Get Cassandra connection."""
    from cassandra.cluster import Cluster
    
    hosts = os.environ.get('CASS_CONTACT_POINTS', '127.0.0.1').split(',')
    port = int(os.environ.get('CASS_PORT', 9042))
    keyspace = os.environ.get('CASS_KEYSPACE', 'blog_data')
    
    cluster = Cluster(hosts, port=port)
    session = cluster.connect()
    
    # Ensure keyspace exists
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(keyspace)
    
    # Ensure table exists
    session.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id int PRIMARY KEY,
            title text,
            content text,
            author text,
            date timestamp
        )
    """)
    
    return cluster, session


def cmd_status(args):
    """Show current migration status."""
    print("\n" + "=" * 60)
    print("MIGRATION STATUS")
    print("=" * 60)
    
    current_phase = os.environ.get('MIGRATION_PHASE', 'mongo_only')
    print(f"\nCurrent Phase: {current_phase}")
    
    # Check MongoDB
    print("\n--- MongoDB ---")
    try:
        client, db = get_mongo_connection()
        posts = db['posts']
        count = posts.count_documents({})
        print(f"✓ Connected")
        print(f"  Posts count: {count}")
        
        # Show sample
        sample = posts.find_one({}, {'_id': 0})
        if sample:
            print(f"  Sample post: ID={sample.get('id')}, Title='{sample.get('title', '')[:30]}...'")
        client.close()
    except Exception as e:
        print(f"✗ Connection failed: {e}")
    
    # Check Cassandra
    print("\n--- Cassandra ---")
    try:
        cluster, session = get_cassandra_connection()
        result = session.execute("SELECT COUNT(*) FROM posts")
        count = result.one()[0]
        print(f"✓ Connected")
        print(f"  Posts count: {count}")
        
        # Show sample
        sample = session.execute("SELECT id, title FROM posts LIMIT 1")
        row = sample.one()
        if row:
            print(f"  Sample post: ID={row.id}, Title='{row.title[:30] if row.title else ''}...'")
        cluster.shutdown()
    except Exception as e:
        print(f"✗ Connection failed: {e}")
    
    print("\n" + "=" * 60)
    print("MIGRATION PHASES:")
    print("-" * 60)
    phases = [
        ("mongo_only", "All reads/writes use MongoDB only"),
        ("dual_write", "Writes go to both DBs, reads from MongoDB"),
        ("read_cassandra", "Writes go to both DBs, reads from Cassandra"),
        ("cassandra_only", "All reads/writes use Cassandra only")
    ]
    for phase, desc in phases:
        marker = "→" if phase == current_phase else " "
        print(f"  {marker} {phase:18} - {desc}")
    print("=" * 60 + "\n")


def cmd_migrate(args):
    """Run the initial data migration from MongoDB to Cassandra."""
    print("\n" + "=" * 60)
    print("MIGRATING DATA: MongoDB → Cassandra")
    print("=" * 60)
    
    try:
        # Connect to both databases
        print("\nConnecting to databases...")
        mongo_client, mongo_db = get_mongo_connection()
        print("✓ MongoDB connected")
        
        cass_cluster, cass_session = get_cassandra_connection()
        print("✓ Cassandra connected")
        
        # Get posts from MongoDB
        posts_collection = mongo_db['posts']
        mongo_posts = list(posts_collection.find({}, {'_id': 0}))
        total = len(mongo_posts)
        
        if total == 0:
            print("\nNo posts found in MongoDB. Nothing to migrate.")
            return
        
        print(f"\nFound {total} posts to migrate.")
        
        if args.dry_run:
            print("\n[DRY RUN] Would migrate the following posts:")
            for post in mongo_posts:
                print(f"  - ID={post.get('id')}: {post.get('title', 'Untitled')}")
            return
        
        # Prepare insert statement
        insert_query = """
            INSERT INTO posts (id, title, content, author, date)
            VALUES (?, ?, ?, ?, ?)
        """
        prepared = cass_session.prepare(insert_query)
        
        # Migrate each post
        migrated = 0
        errors = 0
        
        print("\nMigrating posts...")
        for post in mongo_posts:
            try:
                post_id = post.get('id', 0)
                title = post.get('title', '')
                content = post.get('content', '')
                author = post.get('author', 'Anonymous')
                date = post.get('Date', datetime.now())
                
                if isinstance(date, str):
                    date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
                
                cass_session.execute(prepared, (post_id, title, content, author, date))
                migrated += 1
                print(f"  ✓ Migrated post {post_id}: {title[:40]}...")
                
            except Exception as e:
                errors += 1
                print(f"  ✗ Error migrating post {post.get('id')}: {e}")
        
        print("\n" + "-" * 60)
        print(f"Migration complete!")
        print(f"  Successfully migrated: {migrated}/{total}")
        if errors > 0:
            print(f"  Errors: {errors}")
        
        # Cleanup
        mongo_client.close()
        cass_cluster.shutdown()
        
    except Exception as e:
        print(f"\n✗ Migration failed: {e}")
        import traceback
        traceback.print_exc()


def cmd_verify(args):
    """Verify data integrity between MongoDB and Cassandra."""
    print("\n" + "=" * 60)
    print("VERIFYING DATA INTEGRITY")
    print("=" * 60)
    
    try:
        # Connect to both databases
        mongo_client, mongo_db = get_mongo_connection()
        cass_cluster, cass_session = get_cassandra_connection()
        
        posts_collection = mongo_db['posts']
        
        # Compare counts
        mongo_count = posts_collection.count_documents({})
        cass_result = cass_session.execute("SELECT COUNT(*) FROM posts")
        cass_count = cass_result.one()[0]
        
        print(f"\n--- Record Counts ---")
        print(f"MongoDB:   {mongo_count}")
        print(f"Cassandra: {cass_count}")
        
        if mongo_count == cass_count:
            print("✓ Counts match!")
        else:
            print("✗ Count mismatch!")
        
        # Compare individual records
        print(f"\n--- Data Comparison ---")
        
        mongo_posts = {p['id']: p for p in posts_collection.find({}, {'_id': 0})}
        
        cass_posts = {}
        for row in cass_session.execute("SELECT id, title, content, author, date FROM posts"):
            cass_posts[row.id] = {
                'id': row.id,
                'title': row.title,
                'content': row.content,
                'author': row.author,
                'Date': row.date
            }
        
        all_ids = set(mongo_posts.keys()) | set(cass_posts.keys())
        
        matches = 0
        mismatches = 0
        
        for post_id in sorted(all_ids):
            mongo_post = mongo_posts.get(post_id)
            cass_post = cass_posts.get(post_id)
            
            if not mongo_post:
                print(f"  ✗ Post {post_id}: Missing in MongoDB")
                mismatches += 1
                continue
            
            if not cass_post:
                print(f"  ✗ Post {post_id}: Missing in Cassandra")
                mismatches += 1
                continue
            
            # Compare fields
            issues = []
            if mongo_post.get('title') != cass_post.get('title'):
                issues.append("title")
            if mongo_post.get('content') != cass_post.get('content'):
                issues.append("content")
            if mongo_post.get('author') != cass_post.get('author'):
                issues.append("author")
            
            if issues:
                print(f"  ✗ Post {post_id}: Mismatch in {', '.join(issues)}")
                mismatches += 1
            else:
                matches += 1
        
        print(f"\n--- Summary ---")
        print(f"Matching posts: {matches}")
        print(f"Mismatched posts: {mismatches}")
        
        if mismatches == 0:
            print("\n✓ All data verified successfully!")
        else:
            print("\n✗ Data integrity issues found!")
        
        # Cleanup
        mongo_client.close()
        cass_cluster.shutdown()
        
    except Exception as e:
        print(f"\n✗ Verification failed: {e}")
        import traceback
        traceback.print_exc()


def cmd_set_phase(args):
    """Set the migration phase."""
    valid_phases = ['mongo_only', 'dual_write', 'read_cassandra', 'cassandra_only']
    
    if args.phase not in valid_phases:
        print(f"Invalid phase: {args.phase}")
        print(f"Valid phases: {', '.join(valid_phases)}")
        return
    
    print("\n" + "=" * 60)
    print(f"SETTING MIGRATION PHASE: {args.phase}")
    print("=" * 60)
    
    # Create/update .env file
    env_file = os.path.join(os.path.dirname(__file__), '.env')
    
    env_vars = {}
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    env_vars[key] = value
    
    env_vars['MIGRATION_PHASE'] = args.phase
    
    with open(env_file, 'w') as f:
        for key, value in env_vars.items():
            f.write(f"{key}={value}\n")
    
    print(f"\n✓ Updated .env file with MIGRATION_PHASE={args.phase}")
    print("\nTo apply the change:")
    print("  1. Restart your application")
    print("  2. Or set the environment variable manually:")
    print(f"     export MIGRATION_PHASE={args.phase}")
    
    # Show what to expect
    print("\n" + "-" * 60)
    descriptions = {
        'mongo_only': "All reads and writes will use MongoDB only.",
        'dual_write': "Writes will go to both MongoDB and Cassandra.\nReads will come from MongoDB.",
        'read_cassandra': "Writes will go to both MongoDB and Cassandra.\nReads will come from Cassandra.",
        'cassandra_only': "All reads and writes will use Cassandra only.\nMongoDB will not be used."
    }
    print(f"Phase '{args.phase}':\n  {descriptions[args.phase]}")
    print("=" * 60 + "\n")


def cmd_cleanup(args):
    """Remove MongoDB data after migration is complete."""
    print("\n" + "=" * 60)
    print("CLEANUP: Removing MongoDB Data")
    print("=" * 60)
    
    current_phase = os.environ.get('MIGRATION_PHASE', 'mongo_only')
    
    if current_phase != 'cassandra_only':
        print(f"\n⚠ Warning: Current phase is '{current_phase}'")
        print("  Cleanup should only be run after confirming the migration")
        print("  and setting the phase to 'cassandra_only'.")
        
        if not args.force:
            print("\n  Use --force to proceed anyway.")
            return
    
    try:
        mongo_client, mongo_db = get_mongo_connection()
        posts_collection = mongo_db['posts']
        
        count = posts_collection.count_documents({})
        print(f"\nFound {count} posts in MongoDB to delete.")
        
        if args.dry_run:
            print("\n[DRY RUN] Would delete all posts from MongoDB")
            return
        
        confirm = input("\nAre you sure you want to delete all MongoDB data? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Cleanup cancelled.")
            return
        
        # Delete all posts
        result = posts_collection.delete_many({})
        print(f"\n✓ Deleted {result.deleted_count} posts from MongoDB")
        
        # Optionally drop the collection
        if args.drop_collection:
            mongo_db.drop_collection('posts')
            print("✓ Dropped 'posts' collection")
        
        mongo_client.close()
        
        print("\n" + "-" * 60)
        print("Cleanup complete!")
        print("MongoDB data has been removed.")
        print("You can now remove MongoDB dependencies from requirements.txt")
        print("=" * 60 + "\n")
        
    except Exception as e:
        print(f"\n✗ Cleanup failed: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="MongoDB to Cassandra Migration Controller",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Migration Workflow:
  1. Run 'status' to check current state
  2. Run 'migrate' to copy data to Cassandra
  3. Run 'verify' to ensure data integrity
  4. Run 'set-phase dual_write' to start writing to both DBs
  5. Run 'set-phase read_cassandra' to start reading from Cassandra
  6. Run 'set-phase cassandra_only' when ready to complete migration
  7. Run 'cleanup' to remove MongoDB data
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show migration status')
    
    # Migrate command
    migrate_parser = subparsers.add_parser('migrate', help='Migrate data to Cassandra')
    migrate_parser.add_argument('--dry-run', action='store_true',
                                help='Show what would be migrated without writing')
    
    # Verify command
    verify_parser = subparsers.add_parser('verify', help='Verify data integrity')
    
    # Set phase command
    phase_parser = subparsers.add_parser('set-phase', help='Set migration phase')
    phase_parser.add_argument('phase', 
                              choices=['mongo_only', 'dual_write', 'read_cassandra', 'cassandra_only'],
                              help='Migration phase to set')
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Remove MongoDB data')
    cleanup_parser.add_argument('--dry-run', action='store_true',
                                help='Show what would be deleted without deleting')
    cleanup_parser.add_argument('--force', action='store_true',
                                help='Force cleanup even if not in cassandra_only phase')
    cleanup_parser.add_argument('--drop-collection', action='store_true',
                                help='Also drop the posts collection')
    
    args = parser.parse_args()
    
    if args.command == 'status':
        cmd_status(args)
    elif args.command == 'migrate':
        cmd_migrate(args)
    elif args.command == 'verify':
        cmd_verify(args)
    elif args.command == 'set-phase':
        cmd_set_phase(args)
    elif args.command == 'cleanup':
        cmd_cleanup(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
