from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://localhost:27017/")
db = client['blog_database']
posts_collection = db['posts']

def get_posts(sort_by="date"):
    all_posts = list(posts_collection.find({}, {'_id': 0}))
    if sort_by == "title":
        return sorted(all_posts, key=lambda x: x.get('title', '').lower())
    else:
        return sorted(all_posts, key=lambda x: x.get('Date', datetime.min), reverse=True)

def add_post(new_data):
    new_data['Date'] = datetime.now()
    last_post = posts_collection.find_one(sort=[("id", -1)])
    new_data['id'] = (last_post['id'] + 1) if last_post else 1
    
    if 'author' not in new_data or not new_data['author']:
        new_data['author'] = "Ahmed"
    
    posts_collection.insert_one(new_data)
    
    response_data = new_data.copy()
    if '_id' in response_data:
        del response_data['_id']
    response_data['Date'] = response_data['Date'].strftime("%Y-%m-%d %H:%M:%S")
    return response_data

def get_user_post_counts():
    pipeline = [
        {"$group": {"_id": "$author", "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "author": "$_id", "count": 1}}
    ]
    return list(posts_collection.aggregate(pipeline))