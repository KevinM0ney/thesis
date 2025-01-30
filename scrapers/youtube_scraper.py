from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class YouTubeScraper:
    def __init__(self):
        print("\n✨ Initializing YouTube Scraper...")
        self.api_key = os.getenv('YOUTUBE_API_KEY')
        self.youtube = build('youtube', 'v3', developerKey=self.api_key)
        print(f"🔑 API Key loaded: {'Yes' if self.api_key else 'No'}")

        self.start_date = datetime(2022, 6, 1).strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"📅 Collecting videos from: {self.start_date}")

        # Search terms
        self.search_terms = [
            'artificial intelligence', 'AI technology',
            'OpenAI', 'AI developments', 'AI',
            'future of AI', 'AI ethics'
        ]
        print(f"🔍 Search terms: {', '.join(self.search_terms)}")
        print("=" * 100)

    def fetch_videos(self, search_term, next_page_token=None):
        print(f"\n🔄 Searching for: {search_term}")

        try:
            # Search for videos
            request = self.youtube.search().list(
                part='snippet',
                q=search_term,
                type='video',
                order='date',
                publishedAfter=self.start_date,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()

            video_data = []
            video_ids = []

            for item in response['items']:
                video_ids.append(item['id']['videoId'])

                video_info = {
                    'video_id': item['id']['videoId'],
                    'title': item['snippet']['title'],
                    'description': item['snippet']['description'],
                    'channel_title': item['snippet']['channelTitle'],
                    'channel_id': item['snippet']['channelId'],
                    'published_at': item['snippet']['publishedAt'],
                    'search_term': search_term
                }
                video_data.append(video_info)

            # Get additional video statistics
            if video_ids:
                stats_request = self.youtube.videos().list(
                    part='statistics',
                    id=','.join(video_ids)
                )
                stats_response = stats_request.execute()

                # Add statistics to video data
                for i, stats in enumerate(stats_response['items']):
                    video_data[i].update({
                        'view_count': stats['statistics'].get('viewCount', 0),
                        'like_count': stats['statistics'].get('likeCount', 0),
                        'comment_count': stats['statistics'].get('commentCount', 0)
                    })

            return video_data, response.get('nextPageToken')

        except Exception as e:
            print(f"❌ Error fetching videos: {e}")
            return [], None

    def fetch_comments(self, video_id):
        print(f"\n💬 Fetching comments for video: {video_id}")
        comments = []

        try:
            request = self.youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=100,
                order='relevance'
            )
            response = request.execute()

            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']
                comment_data = {
                    'video_id': video_id,
                    'comment_id': item['id'],
                    'author': comment['authorDisplayName'],
                    'text': comment['textDisplay'],
                    'like_count': comment['likeCount'],
                    'published_at': comment['publishedAt']
                }
                comments.append(comment_data)

            print(f"✅ Collected {len(comments)} comments")

        except Exception as e:
            print(f"❌ Error fetching comments: {e}")

        return comments

    def scrape_all(self):
        print("\n🚀 Starting YouTube scraping process...")
        all_videos = []
        all_comments = []

        for term in self.search_terms:
            print(f"\n📚 Processing search term: {term}")
            print("=" * 100)

            next_page = None
            total_videos = 0

            while True:
                videos, next_page = self.fetch_videos(term, next_page)

                if not videos:
                    break

                for video in videos:
                    print(f"\n📺 Processing video: {video['title'][:100]}...")
                    comments = self.fetch_comments(video['video_id'])
                    all_comments.extend(comments)

                all_videos.extend(videos)
                total_videos += len(videos)

                print(f"\n✅ Processed {len(videos)} videos")
                print(f"📊 Total videos for '{term}': {total_videos}")

                if not next_page:
                    break

                time.sleep(1)  # Respect API quotas

            time.sleep(2)  # Pause between search terms

        return all_videos, all_comments

    def save_to_csv(self, videos, comments, base_path='D:/PycharmProjects/Thesis/data/'):
        # Save videos
        videos_df = pd.DataFrame(videos)
        videos_file = os.path.join(base_path, 'youtube_videos.csv')
        videos_df.to_csv(videos_file, index=False)
        print(f"\n💾 Saved {len(videos)} videos to {videos_file}")

        # Save comments
        comments_df = pd.DataFrame(comments)
        comments_file = os.path.join(base_path, 'youtube_comments.csv')
        comments_df.to_csv(comments_file, index=False)
        print(f"💾 Saved {len(comments)} comments to {comments_file}")

        # Print statistics
        print("\n📊 Data Collection Statistics:")
        print(f"    - Total videos: {len(videos)}")
        print(f"    - Total comments: {len(comments)}")
        print(f"    - Average comments per video: {len(comments) / len(videos):.1f}")
        print(f"    - Search terms covered: {len(videos_df['search_term'].unique())}")
        print(f"    - Date range: {videos_df['published_at'].min()} to {videos_df['published_at'].max()}")


def main():
    print("\n🚀 Starting YouTube AI Content Scraper")
    print("=" * 100)

    try:
        scraper = YouTubeScraper()
        print("\n🔍 Beginning data collection...")
        videos, comments = scraper.scrape_all()
        scraper.save_to_csv(videos, comments)
        print("\n✨ Script completed successfully!")

    except Exception as e:
        print(f"\n❌ An error occurred during execution: {e}")

    finally:
        print("\n👋 Script execution ended")
        print("=" * 100)


if __name__ == "__main__":
    main()