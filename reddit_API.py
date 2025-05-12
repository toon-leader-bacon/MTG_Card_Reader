import praw
import os
import time
import requests
from typing import List, Dict, Optional
from datetime import datetime
import logging


class Reddit_API:
  def __init__(
      self,
      config_section: str = "mtg_card_collector",  # Section name in praw.ini to use for credentials
      request_delay: float = 5.0,
      output_dir: str = "./data/raw/cards/reddit_custom_magic"
  ):
    """
    Initialize Reddit API client for fetching custom MTG cards.

    Args:
        config_section: Section name in praw.ini to use for credentials
        request_delay: Time to wait between API requests in seconds
        output_dir: Directory to save downloaded images
    """
    # Initialize PRAW with the specified config section
    self.reddit = praw.Reddit(config_section)
    self.reddit.read_only = True
    self.request_delay = request_delay
    self.output_dir = output_dir
    self._setup_logging()
    self._ensure_output_dir()

  def _setup_logging(self):
    """Configure logging for the API client"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    self.logger = logging.getLogger(__name__)

  def _ensure_output_dir(self):
    """Create output directory if it doesn't exist"""
    os.makedirs(self.output_dir, exist_ok=True)

  def _is_image_post(self, post) -> bool:
    """Check if a post contains an image"""
    return hasattr(post, 'post_hint') and post.post_hint == 'image'

  def _download_image(self, url: str, post_id: str) -> Optional[str]:
    """
    Download an image from a URL and save it to the output directory.

    Returns:
        Path to saved image if successful, None otherwise
    """
    try:
      response = requests.get(url, stream=True)
      response.raise_for_status()

      # Get file extension from URL or default to .png
      ext = os.path.splitext(url)[1] or '.png'
      filename = f"{post_id}{ext}"
      filepath = os.path.join(self.output_dir, filename)

      with open(filepath, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
          f.write(chunk)

      return filepath
    except Exception as e:
      self.logger.error(f"Failed to download image from {url}: {e}")
      return None

  def fetch_recent_cards(
      self,
      subreddit: str = "custom_magic",
      limit: int = 100,
      time_filter: str = "month"
  ) -> List[Dict]:
    """
    Fetch recent custom card posts from specified subreddit.

    Args:
        subreddit: Subreddit to fetch from
        limit: Maximum number of posts to fetch
        time_filter: One of (hour, day, week, month, year, all)

    Returns:
        List of dictionaries containing post metadata and image paths
    """
    self.logger.info(f"Fetching {limit} recent posts from r/{subreddit}")
    cards = []

    try:
      subreddit = self.reddit.subreddit(subreddit)
      for post in subreddit.top(time_filter=time_filter, limit=limit):
        if self._is_image_post(post):
          self.logger.info(f"Processing post: {post.title}")

          # Download image
          image_path = self._download_image(post.url, post.id)
          if image_path:
            cards.append({
                'id': post.id,
                'title': post.title,
                'url': post.url,
                'created_utc': datetime.fromtimestamp(post.created_utc),
                'image_path': image_path
            })

          # Respect rate limits
          time.sleep(self.request_delay)

      self.logger.info(f"Successfully processed {len(cards)} card images")
      return cards

    except Exception as e:
      self.logger.error(f"Error fetching posts: {e}")
      return []


if __name__ == "__main__":
  # Example usage
  api = Reddit_API(
      config_section="mtg_card_collector",
      request_delay=5.0
  )

  # Fetch recent cards
  cards = api.fetch_recent_cards(limit=10)
  for card in cards:
    print(f"Downloaded: {card['title']} -> {card['image_path']}")
