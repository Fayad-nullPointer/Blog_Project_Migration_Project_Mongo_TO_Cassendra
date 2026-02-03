# MongoDB -> Cassandra migration

This repository includes a small migration script: `migrate_mongo_to_cassandra.py`.

Quick usage

1. Ensure Cassandra is running (you mentioned you've pulled the image). Example using Docker:

```bash
# run Cassandra (single-node dev)
docker run --name cassandra -p 9042:9042 -d cassandra:latest
```

2. Install Python deps (prefer a venv):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. Run migration (example migrating all collections from `test` DB to keyspace `migrated`):

```bash
python migrate_mongo_to_cassandra.py --mongodb-uri mongodb://localhost:27017 --mongo-db test --cass-hosts 127.0.0.1 --cass-keyspace migrated
```

Options

- `--collections` comma-separated list of collections to migrate (default: all)
- `--batch-size` number of statements per Cassandra batch (default: 50)
- `--dry-run` show actions without writing to Cassandra

Notes

- The script stores full Mongo documents as JSON strings in Cassandra tables named after the Mongo collection.
- Mongo `_id` is stringified and used as the primary key in Cassandra.
- This is a simple approach to migrate data for read-only archival or further transformation; adjust the schema if you need column-level queries in Cassandra.
