import praw
import os
import time
import requests
from typing import List, Dict, Optional, Generator
from datetime import datetime, timedelta
import logging
import calendar


class Reddit_API:
  def __init__(
      self,
      config_section: str = "mtg_card_collector",  # Section name in praw.ini to use for credentials
      request_delay: float = 1.0,  # Changed default to 1 second
      output_dir: str = "./data/raw/cards/reddit_custommagic"
  ):
    """
    Initialize Reddit API client for fetching custom MTG cards.

    Args:
        config_section: Section name in praw.ini to use for credentials
        request_delay: Minimum time to wait between API requests in seconds
        output_dir: Directory to save downloaded images
    """
    # Initialize PRAW with the specified config section
    self.reddit = praw.Reddit(config_section)
    self.reddit.read_only = True
    self.request_delay = request_delay
    self.output_dir = output_dir
    self._setup_logging()
    self._ensure_output_dir()
    self._last_request_time = 0  # Track last request time for rate limiting

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

  def _rate_limit(self):
    """
    Enforce rate limiting by ensuring at least request_delay seconds
    have passed since the last request.
    """
    current_time = time.time()
    time_since_last_request = current_time - self._last_request_time
    
    if time_since_last_request < self.request_delay:
      sleep_time = self.request_delay - time_since_last_request
      self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
      time.sleep(sleep_time)
    
    self._last_request_time = time.time()

  def _download_image(self, url: str, post_id: str) -> Optional[str]:
    """
    Download an image from a URL and save it to the output directory.

    Returns:
        Path to saved image if successful, None otherwise
    """
    try:
      self._rate_limit()  # Rate limit the image download request
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
      subreddit: str = "custommagic",
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
      self._rate_limit()  # Rate limit the subreddit fetch
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

      self.logger.info(f"Successfully processed {len(cards)} card images")
      return cards

    except Exception as e:
      self.logger.error(f"Error fetching posts: {e}")
      return []

  def fetch_posts_by_date_range(
      self,
      subreddit: str,
      start_date: datetime,
      end_date: datetime,
      batch_size: int = 100
  ) -> Generator[List[Dict], None, None]:
    """
    Fetch posts from a subreddit within a specific date range, yielding batches of results.
    
    Args:
        subreddit: Subreddit to fetch from
        start_date: Start date for the search
        end_date: End date for the search
        batch_size: Number of posts to fetch in each batch
        
    Yields:
        Lists of dictionaries containing post metadata and image paths
    """
    self.logger.info(f"Fetching posts from r/{subreddit} between {start_date} and {end_date}")
    
    try:
      self._rate_limit()  # Rate limit the subreddit fetch
      subreddit = self.reddit.subreddit(subreddit)
      current_batch = []
      
      # Convert dates to timestamps for Reddit's search
      start_timestamp = int(start_date.timestamp())
      end_timestamp = int(end_date.timestamp())
      
      # Use search with timestamp parameters
      search_query = f"timestamp:{start_timestamp}..{end_timestamp}"
      
      for post in subreddit.search(search_query, sort='new', time_filter='all', limit=None):
        if self._is_image_post(post):
          self.logger.info(f"Processing post: {post.title}")
          
          # Download image
          image_path = self._download_image(post.url, post.id)
          if image_path:
            current_batch.append({
                'id': post.id,
                'title': post.title,
                'url': post.url,
                'created_utc': datetime.fromtimestamp(post.created_utc),
                'image_path': image_path
            })
          
          # Yield batch when it reaches the desired size
          if len(current_batch) >= batch_size:
            yield current_batch
            current_batch = []
      
      # Yield any remaining posts
      if current_batch:
        yield current_batch
        
    except Exception as e:
      self.logger.error(f"Error fetching posts: {e}")
      yield []

  def fetch_monthly_batches(
      self,
      subreddit: str,
      years_back: int = 10,
      batch_size: int = 100
  ) -> Generator[tuple[datetime, List[Dict]], None, None]:
    """
    Fetch posts in monthly batches over a specified number of years.
    
    Args:
        subreddit: Subreddit to fetch from
        years_back: Number of years to look back
        batch_size: Number of posts to fetch in each batch
        
    Yields:
        Tuples of (month_start_date, list of posts) for each month
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * years_back)
    
    current_date = start_date
    while current_date < end_date:
      # Calculate the last day of the current month
      last_day = calendar.monthrange(current_date.year, current_date.month)[1]
      month_end = datetime(current_date.year, current_date.month, last_day, 23, 59, 59)
      
      self.logger.info(f"Processing month: {current_date.strftime('%Y-%m')}")
      
      # Fetch posts for this month
      for batch in self.fetch_posts_by_date_range(
          subreddit,
          current_date,
          month_end,
          batch_size
      ):
        yield current_date, batch
      
      # Move to the first day of next month
      if current_date.month == 12:
        current_date = datetime(current_date.year + 1, 1, 1)
      else:
        current_date = datetime(current_date.year, current_date.month + 1, 1)


if __name__ == "__main__":
  # Example usage
  api = Reddit_API(
      config_section="mtg_card_collector",
      request_delay=5.0
  )

  # Fetch posts in monthly batches over the last 10 years
  for month_start, batch in api.fetch_monthly_batches("custommagic", years_back=1):
    print(f"\nProcessing month: {month_start.strftime('%Y-%m')}")
    print(f"Found {len(batch)} posts in this batch")
    for card in batch:
      print(f"Downloaded: {card['title']} -> {card['image_path']}")
