# Blog Application - MongoDB to Cassandra Migration Guide

This guide explains how to migrate the blog application from MongoDB to Cassandra using a phased approach.

## Project Features

The blog application supports:
- **Multiple authors/commenters** - Each post has an author
- **Main feed** - Display all posts
- **Sorting options**:
  - By date (newest first)
  - By title (alphabetical A-Z)
- **Post count per author** - Shows how many posts each author has

## Architecture

### Files Overview

| File | Purpose |
|------|---------|
| `app.py` | Flask web application |
| `data.py` | Original MongoDB-only data layer |
| `data_migration.py` | Migration-aware data layer (supports all phases) |
| `data_cassandra_only.py` | Final Cassandra-only data layer |
| `migration_controller.py` | CLI tool to manage migration process |
| `migrate_mongo_to_cassandra.py` | Standalone migration script |
| `Varify_migration.py` | Data verification script |

## Migration Strategy

### Phase 1: MongoDB Only (`mongo_only`)

Initial state - all reads and writes use MongoDB.

```bash
export MIGRATION_PHASE=mongo_only
python app.py
```

### Phase 2: Dual Writes (`dual_write`)

Start writing to both databases while still reading from MongoDB.

```bash
# First, migrate existing data
python migration_controller.py migrate

# Verify the migration
python migration_controller.py verify

# Switch to dual-write mode
export MIGRATION_PHASE=dual_write
python app.py
```

### Phase 3: Read from Cassandra (`read_cassandra`)

Continue writing to both databases, but read from Cassandra.

```bash
# Switch to reading from Cassandra
export MIGRATION_PHASE=read_cassandra
python app.py
```

Test thoroughly to ensure Cassandra reads work correctly.

### Phase 4: Cassandra Only (`cassandra_only`)

Final state - all reads and writes use Cassandra only.

```bash
# Switch to Cassandra only
export MIGRATION_PHASE=cassandra_only
python app.py
```

### Phase 5: Cleanup

Remove MongoDB data and dependencies.

```bash
# Remove MongoDB data
python migration_controller.py cleanup

# Replace data layer
mv data_migration.py data_migration.py.bak
mv data_cassandra_only.py data_migration.py

# Update requirements.txt to remove pymongo
```

## Using the Migration Controller

The `migration_controller.py` provides commands to manage the migration:

```bash
# Check current status
python migration_controller.py status

# Migrate data from MongoDB to Cassandra
python migration_controller.py migrate
python migration_controller.py migrate --dry-run  # Preview only

# Verify data integrity
python migration_controller.py verify

# Set migration phase
python migration_controller.py set-phase dual_write
python migration_controller.py set-phase read_cassandra
python migration_controller.py set-phase cassandra_only

# Cleanup MongoDB (after migration complete)
python migration_controller.py cleanup
python migration_controller.py cleanup --dry-run  # Preview only
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MIGRATION_PHASE` | `mongo_only` | Current migration phase |
| `MONGODB_URI` | `mongodb://localhost:27017/` | MongoDB connection string |
| `MONGO_DB` | `blog_database` | MongoDB database name |
| `CASS_CONTACT_POINTS` | `127.0.0.1` | Cassandra hosts (comma-separated) |
| `CASS_PORT` | `9042` | Cassandra port |
| `CASS_KEYSPACE` | `blog_data` | Cassandra keyspace |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Main blog page |
| `/api/posts` | GET | Get all posts (supports `?sort=date` or `?sort=title`) |
| `/api/posts` | POST | Create a new post |
| `/api/stats` | GET | Get post counts per author |
| `/api/migration/status` | GET | Get current migration status |

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start MongoDB and Cassandra

```bash
# MongoDB
mongod

# Cassandra (using Docker)
docker run --name cassandra -p 9042:9042 -d cassandra:latest
```

### 3. Insert Sample Data

```bash
python insert_data.py
```

### 4. Run the Application

```bash
# Start with MongoDB
export MIGRATION_PHASE=mongo_only
python app.py
```

### 5. Perform Migration

```bash
# Migrate data
python migration_controller.py migrate

# Verify
python migration_controller.py verify

# Switch to dual-write
python migration_controller.py set-phase dual_write
# Restart app

# Test, then switch to reading from Cassandra
python migration_controller.py set-phase read_cassandra
# Restart app

# Finally, switch to Cassandra only
python migration_controller.py set-phase cassandra_only
# Restart app
```

## Rollback Strategy

If issues occur during migration:

1. **During dual_write**: Simply switch back to `mongo_only`
2. **During read_cassandra**: Switch back to `dual_write` to continue using MongoDB reads
3. **After cassandra_only**: You'll need to restore from MongoDB backup

## Best Practices

1. Always run migrations during low-traffic periods
2. Use `--dry-run` first to preview changes
3. Verify data integrity after each phase
4. Keep MongoDB running until you're confident in Cassandra
5. Take backups before cleanup
