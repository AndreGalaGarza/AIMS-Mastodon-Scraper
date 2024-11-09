import pandas as pd
import json
import os
from mastodon import Mastodon as MastodonAPI
from datetime import datetime

class Mastodon:
    def __init__(self, client_id, client_secret, access_token, api_base_url='https://mastodon.social'):
        """
        Initializes the Mastodon class with API credentials and base URL.

        Parameters:
        - client_id (str): The client ID for Mastodon API.
        - client_secret (str): The client secret for Mastodon API.
        - access_token (str): The access token for Mastodon API.
        - api_base_url (str): The base URL of the Mastodon instance (default is 'https://mastodon.social').
        """
        self.api = MastodonAPI(
            client_id=client_id,
            client_secret=client_secret,
            access_token=access_token,
            api_base_url=api_base_url
        )

    def _simplify_toot(self, toot):
        """
        Simplifies the toot dictionary to include only the specified fields.

        Parameters:
        - toot (dict): The original toot dictionary from Mastodon.

        Returns:
        - dict: A simplified dictionary containing only the specified fields.

        See a list of all return values here:
        https://mastodonpy.readthedocs.io/en/stable/02_return_values.html
        """
        account = toot.get('account')

        simplified_toot = {
            'id': toot.get('id'),
            'uri': toot.get('uri'),
            'url': toot.get('url'),
            'account': account,  # Full user dictionary for the account that posted the status
            'in_reply_to_id': toot.get('in_reply_to_id'),
            'in_reply_to_account_id': toot.get('in_reply_to_account_id'),
            'reblog': toot.get('reblog'),  # Original toot dict if this is a reblog
            'content': toot.get('content'),
            'created_at': toot.get('created_at'),
            'reblogs_count': toot.get('reblogs_count'),
            'favourites_count': toot.get('favourites_count'),
            'reblogged': toot.get('reblogged'),
            'favourited': toot.get('favourited'),
            'sensitive': toot.get('sensitive'),
            'spoiler_text': toot.get('spoiler_text'),
            'visibility': toot.get('visibility'),
            'mentions': toot.get('mentions'),  # List of mentioned users dicts
            'media_attachments': toot.get('media_attachments'),  # List of media dicts for attached files
            'emojis': toot.get('emojis'),  # List of custom emojis used in the toot
            'tags': toot.get('tags'),  # List of hashtags used in the toot
            'bookmarked': toot.get('bookmarked'),
            'application': toot.get('application'),  # Application dict for the client used
            'language': toot.get('language'),
            'muted': toot.get('muted'),
            'pinned': toot.get('pinned'),
            'replies_count': toot.get('replies_count'),
            'card': toot.get('card'),  # Preview card for links, if present
            'poll': toot.get('poll')  # Poll dict if a poll is attached
        }

        return simplified_toot

    def search_posts(self, search_query, num_posts_per_query=5):
        """
        Searches for posts based on a search query and saves results to both a CSV and a JSON file.

        Parameters:
        - search_query (str): The search query for finding posts.
        - num_posts_per_query (int): Number of posts to fetch per query (default is 5).
        """

        def datetime_converter(obj):
            """Helper function to convert datetime objects to strings for JSON serialization."""
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")

        # Search posts using the specified query
        search_results = self.api.timeline_hashtag(search_query, limit=num_posts_per_query)
        toots_list = [self._simplify_toot(toot) for toot in search_results]
        
        # Create DataFrame and save as CSV
        df = pd.DataFrame(toots_list)
        file_name = search_query.replace(' ', '_')
        if not os.path.exists("posts/csv"):
            os.makedirs("posts/csv")
        csv_file_path = os.path.join("posts/csv", file_name + '.csv')
        df.to_csv(csv_file_path, index=False)
        
        # Convert toots_list to JSON format with datetime conversion
        if not os.path.exists("posts/json"):
            os.makedirs("posts/json")
        json_file_path = os.path.join("posts/json", file_name + '.json')
        with open(json_file_path, 'w', encoding='utf-8') as json_file:
            json_data = json.dumps(toots_list, indent=4, ensure_ascii=False, default=datetime_converter)
            json_file.write(json_data)
        
        print("Search results saved to:", csv_file_path, "and", json_file_path)

# Example of class usage.
# Place your own Mastodon credentials here.
mastodon = Mastodon(
    client_id='',
    client_secret='',
    access_token=''
)

# Dictionary of categories and keywords
nuclear_categories = {
    "safety": ["risk", "dangerous", "accident", "radiation"],
    "economy": ["affordable", "cheap", "expensive", "pricy"],
    "technology": ["fusion", "advanced", "future", "SMR"],
    "waste": ["radiotoxic", "disposal", "spent-fuel", "contamination"],
    "energy": ["green", "carbon-free", "eco-friendly"]
}

# Iterate through each category word and its keywords,
# performing a search with each word plus the word "nuclear".
query = ""
num_posts = 10
for category, keywords in nuclear_categories.items():
    # Search with category word
    query = "nuclear " + category
    mastodon.search_posts(query, num_posts)
    # Search with related keywords
    for keyword in keywords:
        query = "nuclear " + keyword
        mastodon.search_posts(query, num_posts)