async function loadPosts() {
    const sortSelect = document.getElementById('sort-select');
    const sortType = sortSelect ? sortSelect.value : 'date';
    const response = await fetch(`/api/posts?sort=${sortType}`);
    const posts = await response.json();
    // Fetch stats to get post counts per user
    const statsResponse = await fetch('/api/stats');
    const stats = await statsResponse.json();
    const authorCounts = {};
    stats.forEach(user => {
        authorCounts[user.author] = user.count;
    });
    const container = document.getElementById('blog-container');
    container.innerHTML = posts.map(post => {
        const count = authorCounts[post.author] || 1;
        return `
        <article class="post">
            <h2>${post.title}</h2>
            <p>${post.content}</p>
            <div class="post-meta">
                <small>By: ${post.author} (${count} post${count > 1 ? 's' : ''})</small> |
                <small>${post.Date}</small>
            </div>
        </article>
        `;
    }).join('');
}

// Removed loadStats, as stats are now shown per post

async function submitPost() {
    const userInput = document.getElementById('username');
    const titleInput = document.getElementById('title');
    const contentInput = document.getElementById('content');

    await fetch('/api/posts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
            author: userInput.value || 'Anonymous',
            title: titleInput.value, 
            content: contentInput.value
        })
    });
    
    titleInput.value = '';
    contentInput.value = '';
    loadPosts();
}

window.onload = () => {
    loadPosts();
};