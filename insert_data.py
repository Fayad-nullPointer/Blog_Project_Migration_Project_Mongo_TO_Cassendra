"""
Sample data insertion script for the blog application.
Run this to populate your MongoDB with test data.
"""
from pymongo import MongoClient
from datetime import datetime, timedelta
import random

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client['blog_database']
posts_collection = db['posts']

# Sample data
sample_posts = [
    {
        "id": 1,
        "title": "Welcome to My Blog",
        "content": "This is the first post on my blog. I'm excited to share my thoughts and ideas with you!",
        "author": "Ahmed",
        "Date": datetime.now() - timedelta(days=10)
    },
    {
        "id": 2,
        "title": "Introduction to Python",
        "content": "Python is a powerful and versatile programming language. In this post, I'll cover the basics of Python programming.",
        "author": "Sarah",
        "Date": datetime.now() - timedelta(days=8)
    },
    {
        "id": 3,
        "title": "Web Development with Flask",
        "content": "Flask is a lightweight web framework for Python. It's perfect for building small to medium-sized web applications.",
        "author": "Ahmed",
        "Date": datetime.now() - timedelta(days=6)
    },
    {
        "id": 4,
        "title": "Database Migration Best Practices",
        "content": "When migrating databases, it's important to plan carefully and test thoroughly. Here are some best practices to follow.",
        "author": "John",
        "Date": datetime.now() - timedelta(days=4)
    },
    {
        "id": 5,
        "title": "Understanding NoSQL Databases",
        "content": "NoSQL databases like MongoDB and Cassandra offer flexible schemas and horizontal scalability. Let's explore when to use them.",
        "author": "Sarah",
        "Date": datetime.now() - timedelta(days=2)
    },
    {
        "id": 6,
        "title": "Building RESTful APIs",
        "content": "REST APIs are the backbone of modern web applications. Learn how to design and implement clean, efficient APIs.",
        "author": "Ahmed",
        "Date": datetime.now() - timedelta(days=1)
    },
]

def insert_sample_data():
    """Insert sample posts into MongoDB."""
    # Clear existing data
    posts_collection.delete_many({})
    print("Cleared existing posts.")
    
    # Insert sample posts
    result = posts_collection.insert_many(sample_posts)
    print(f"Inserted {len(result.inserted_ids)} sample posts.")
    
    # Display inserted posts
    print("\nInserted posts:")
    for post in sample_posts:
        print(f"  - [{post['id']}] {post['title']} by {post['author']}")
    
    print("\n✓ Sample data insertion complete!")

if __name__ == '__main__':
    try:
        insert_sample_data()
    except Exception as e:
        print(f"Error inserting sample data: {e}")