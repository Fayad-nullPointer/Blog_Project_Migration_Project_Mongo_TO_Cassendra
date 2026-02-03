"""
Blog Application with MongoDB to Cassandra Migration Support

This Flask application supports a phased migration strategy:
1. MONGO_ONLY - Initial phase, all operations use MongoDB
2. DUAL_WRITE - Writes go to both databases, reads from MongoDB
3. READ_CASSANDRA - Writes go to both, reads from Cassandra
4. CASSANDRA_ONLY - Final phase, MongoDB is removed

Set MIGRATION_PHASE environment variable to control the phase.
"""

from flask import Flask, render_template, jsonify, request
import data_migration as data

app = Flask(__name__)


@app.route('/')
def index():
    """Render the main blog page."""
    return render_template('index.html')


@app.route('/api/posts', methods=['GET'])
def get_posts():
    """
    Get all blog posts.
    
    Query parameters:
    - sort: 'date' (default, newest first) or 'title' (alphabetical A-Z)
    """
    sort_type = request.args.get('sort', 'date')
    posts = data.get_posts(sort_by=sort_type)
    return jsonify(posts)


@app.route('/api/posts', methods=['POST'])
def create_post():
    """
    Create a new blog post.
    
    Expected JSON body:
    - title: Post title (required)
    - content: Post content (required)
    - author: Author name (optional, defaults to 'Anonymous')
    """
    new_post = request.json
    if not new_post or 'title' not in new_post or 'content' not in new_post:
        return jsonify({"error": "Invalid data. 'title' and 'content' are required."}), 400
    result = data.add_post(new_post)
    return jsonify(result), 201


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get post counts per author."""
    return jsonify(data.get_user_post_counts())


@app.route('/api/migration/status', methods=['GET'])
def migration_status():
    """Get current migration phase and database status."""
    return jsonify(data.get_migration_status())


if __name__ == '__main__':
    print("\n" + "=" * 60)
    print("Blog Application Starting")
    print("=" * 60)
    status = data.get_migration_status()
    print(f"Migration Phase: {status['phase']}")
    print(f"MongoDB Connected: {status['mongodb_connected']}")
    print(f"Cassandra Connected: {status['cassandra_connected']}")
    print("=" * 60 + "\n")
    app.run(debug=True)