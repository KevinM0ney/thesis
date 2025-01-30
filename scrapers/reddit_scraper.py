import praw
import pandas as pd
import time
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class RedditScraper:
    def __init__(self):
        print("\n✨ Initializing Reddit Scraper...")

        # Initialize Reddit instance
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent="Research Script 1.0"
        )

        print(f"🔑 Reddit connection: {'Successful' if self.reddit.read_only else 'Failed'}")

        # Subreddits to scrape
        self.subreddits = [
            'ArtificialInteligence', 'ChatGPT',
            'italy', 'ItalyInformatica', 'Universitaly'
        ]

        self.start_date = int(datetime(2022, 6, 1).timestamp())
        print(f"📅 Collecting posts from: {datetime.fromtimestamp(self.start_date)}")
        print(f"📊 Monitoring subreddits: {', '.join(self.subreddits)}")
        print("=" * 100)

    def fetch_posts(self, subreddit_name):
        print(f"\n🔄 Fetching posts from r/{subreddit_name}...")
        subreddit = self.reddit.subreddit(subreddit_name)

        posts_data = []
        try:
            # Get all posts from the subreddit
            for post in subreddit.new(limit=None):
                # Check if post is after our start date
                if post.created_utc < self.start_date:
                    continue

                print(f"\n📝 Processing post: {post.title[:100]}...")

                # Get post data
                post_data = {
                    'subreddit': subreddit_name,
                    'title': post.title,
                    'author': str(post.author),
                    'text': post.selftext,
                    'url': post.url,
                    'post_date': datetime.fromtimestamp(post.created_utc),
                    'score': post.score,
                    'upvote_ratio': post.upvote_ratio,
                    'num_comments': post.num_comments,
                    'post_id': post.id,
                    'post_type': 'text' if post.is_self else 'link'
                }

                # Get top-level comments
                post.comments.replace_more(limit=0)  # Remove MoreComments objects
                comments = []
                for comment in post.comments:
                    comment_data = {
                        'post_id': post.id,
                        'comment_id': comment.id,
                        'author': str(comment.author),
                        'text': comment.body,
                        'score': comment.score,
                        'comment_date': datetime.fromtimestamp(comment.created_utc)
                    }
                    comments.append(comment_data)

                posts_data.append({
                    'post': post_data,
                    'comments': comments
                })

                print(f"✅ Collected {len(comments)} comments")
                print(f"📊 Post score: {post.score}, Comments: {post.num_comments}")

                # Respect rate limits
                time.sleep(1)

        except Exception as e:
            print(f"❌ Error processing subreddit {subreddit_name}: {e}")

        return posts_data

    def scrape_all(self):
        print("\n🚀 Starting Reddit scraping process...")
        all_data = []

        for subreddit in self.subreddits:
            print(f"\n📚 Processing r/{subreddit}...")
            print("=" * 100)

            subreddit_data = self.fetch_posts(subreddit)
            all_data.extend(subreddit_data)

            print(f"✅ Completed r/{subreddit}: collected {len(subreddit_data)} posts")
            time.sleep(2)  # Pause between subreddits

        return all_data

    def save_to_csv(self, data, base_path='D:/PycharmProjects/Thesis/data/'):
        if not data:
            print("❌ No data to save")
            return

        # Create separate DataFrames for posts and comments
        posts_data = []
        comments_data = []

        for item in data:
            posts_data.append(item['post'])
            comments_data.extend(item['comments'])

        # Save posts
        posts_df = pd.DataFrame(posts_data)
        posts_file = os.path.join(base_path, 'reddit_posts.csv')
        posts_df.to_csv(posts_file, index=False)
        print(f"\n💾 Saved {len(posts_data)} posts to {posts_file}")

        # Save comments
        comments_df = pd.DataFrame(comments_data)
        comments_file = os.path.join(base_path, 'reddit_comments.csv')
        comments_df.to_csv(comments_file, index=False)
        print(f"💾 Saved {len(comments_data)} comments to {comments_file}")

        # Print statistics
        print("\n📊 Data Collection Statistics:")
        print(f"    - Total posts: {len(posts_data)}")
        print(f"    - Total comments: {len(comments_data)}")
        print(f"    - Average comments per post: {len(comments_data) / len(posts_data):.1f}")
        print(f"    - Subreddits covered: {len(posts_df['subreddit'].unique())}")
        print(f"    - Date range: {posts_df['post_date'].min()} to {posts_df['post_date'].max()}")


def main():
    print("\n🚀 Starting Reddit AI Content Scraper")
    print("=" * 100)

    try:
        scraper = RedditScraper()
        print("\n🔍 Beginning data collection...")
        data = scraper.scrape_all()
        scraper.save_to_csv(data)
        print("\n✨ Script completed successfully!")

    except Exception as e:
        print(f"\n❌ An error occurred during execution: {e}")

    finally:
        print("\n👋 Script execution ended")
        print("=" * 100)


if __name__ == "__main__":
    main()