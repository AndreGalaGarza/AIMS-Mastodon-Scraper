import pandas as pd
import glob
import os
import re
from mastodon import Mastodon as MastodonAPI

class Mastodon:
    def __init__(self, client_id, client_secret, access_token, api_base_url='https://mastodon.social'):
        """
        Initializes the Mastodon class with API credentials and base URL.

        Parameters:
        - client_id (str): The client ID for Mastodon API.
        - client_secret (str): The client secret for Mastodon API.
        - access_token (str): The access token for Mastodon API.
        - api_base_url (str): The base URL of the Mastodon instance (default is 'https://mastodon.social').

        See a list of all return values here:
        https://mastodonpy.readthedocs.io/en/stable/02_return_values.html

        A post on Mastodon is called a "toot".
        Toot dictionary:
        {
            'id': # Numerical id of this toot
            'uri': # Descriptor for the toot
                # EG 'tag:mastodon.social,2016-11-25:objectId=<id>:objectType=Status'
            'url': # URL of the toot
            'account': # User dict for the account which posted the status
            'in_reply_to_id': # Numerical id of the toot this toot is in response to
            'in_reply_to_account_id': # Numerical id of the account this toot is in response to
            'reblog': # Denotes whether the toot is a reblog. If so, set to the original toot dict.
            'content': # Content of the toot, as HTML: '<p>Hello from Python</p>'
            'created_at': # Creation time
            'reblogs_count': # Number of reblogs
            'favourites_count': # Number of favourites
            'reblogged': # Denotes whether the logged in user has boosted this toot
            'favourited': # Denotes whether the logged in user has favourited this toot
            'sensitive': # Denotes whether media attachments to the toot are marked sensitive
            'spoiler_text': # Warning text that should be displayed before the toot content
            'visibility': # Toot visibility ('public', 'unlisted', 'private', or 'direct')
            'mentions': # A list of users dicts mentioned in the toot, as Mention dicts
            'media_attachments': # A list of media dicts of attached files
            'emojis': # A list of custom emojis used in the toot, as Emoji dicts
            'tags': # A list of hashtag used in the toot, as Hashtag dicts
            'bookmarked': # True if the status is bookmarked by the logged in user, False if not.
            'application': # Application dict for the client used to post the toot (Does not federate
                        # and is therefore always None for remote toots, can also be None for
                        # local toots for some legacy applications).
            'language': # The language of the toot, if specified by the server,
                        # as ISO 639-1 (two-letter) language code.
            'muted': # Boolean denoting whether the user has muted this status by
                    # way of conversation muting
            'pinned': # Boolean denoting whether or not the status is currently pinned for the
                    # associated account.
            'replies_count': # The number of replies to this status.
            'card': # A preview card for links from the status, if present at time of delivery,
                    # as card dict.
            'poll': # A poll dict if a poll is attached to this status.
        }
        """
        self.api = MastodonAPI(
            client_id=client_id,
            client_secret=client_secret,
            access_token=access_token,
            api_base_url=api_base_url
        )

    # Other class member variables
    query_list = []
    epoch_mode = False
    epoch_num = 0
    epoch_df = pd.DataFrame()
    summary_df = pd.DataFrame()
    stats_df = pd.DataFrame()

    def __save_csv(self, df, path, filename):
        """
        Helper function to save a Pandas DataFrame to a CSV file.
        Parameters:
        - df: The Pandas DataFrame.
        - path: The folders leading to the CSV. Do not put a backslash at the end!
        - filename: The name of the CSV itself. Does not include the extension '.csv'.
        """
        if not os.path.exists(path):
            os.makedirs(path)
        csv_file_path = os.path.join(path, filename + '.csv')
        df.to_csv(csv_file_path, index=False)

        print(f"DataFrame saved to path {csv_file_path} with {df.shape[0]} entries")
        
    def get_list_of_queries(self, query_list_filename):
        """
        Obtains a list of search queries to use.
        This list should be in the form of a text file, with queries separated by line.
        Parameters:
        - query_list_filename (str): The filename of the query list, in the form "[name].txt".
        """
        query_list = []
        try:
            with open(query_list_filename, 'r') as file:
                for line in file:
                    query_list.append(line.strip())
        except FileNotFoundError:
            print(f"Error: File {query_list_filename} not found.")
        self.query_list = query_list

    def search_one_query(self, search_query, num_posts_per_query=20, start_id=None, end_id=None):
        """
        Searches for posts based on a single search query and saves results to a CSV file.
        Parameters:
        - search_query (str): The search query for finding posts.
        - num_posts_per_query (int): Number of posts to fetch per query.
            Default is 20 posts per query. Max is 40 posts per query.
        - start_id (int): The ID number to start at (inclusive). Returns results immediately newer than this ID.
            In effect, sets a cursor at this ID and paginates forward.
        - end_id (int): The ID number to end at (inclusive). All results returned will be lesser than this ID. 
            In effect, sets an upper bound on results.
        """
        # Clamp num_posts_per_query between 0 and 40 posts
        num_posts_per_query = max(0, min(num_posts_per_query, 40))

        # Increase the bounds defined by start_id and end_id
        if start_id is not None:
            start_id = start_id - 1
        if end_id is not None:
            end_id = end_id + 1

        # Search posts using the specified query
        search_results = self.api.timeline_hashtag(search_query, limit=num_posts_per_query,
                                                   min_id=start_id, max_id=end_id)
        toots_list = [toot for toot in search_results]
        
        # Create DataFrame and save as CSV
        df = pd.DataFrame(toots_list)

        if self.epoch_mode:
            print(f"Finished searching for query \"{search_query}\" with {df.shape[0]} results")
        else:
            self.__save_csv(df, "data/posts", search_query.replace(' ', '_'))
        
        return df

    def search_list_of_queries(self, num_posts_per_query=20, start_id=None, end_id=None):
        """
        Searches for posts based on a list of search queries and saves results to a CSV file.
        Parameters:
        - num_posts_per_query (int): Number of posts to fetch per query.
            Default is 20 posts per query. Max is 40 posts per query.
        - start_id (int): The ID number to start at (inclusive). Returns results immediately newer than this ID.
            In effect, sets a cursor at this ID and paginates forward.
        - end_id (int): The ID number to end at (inclusive). All results returned will be lesser than this ID. 
            In effect, sets an upper bound on results.
        """
        # Clamp num_posts_per_query between 0 and 40 posts
        num_posts_per_query = max(0, min(num_posts_per_query, 40))

        # Create an empty list to collect valid query DataFrames
        valid_dataframes = []

        # Search using every query in the query list
        for query in self.query_list:
            query_df = self.search_one_query(
                search_query=query,
                num_posts_per_query=num_posts_per_query,
                start_id=start_id,
                end_id=end_id
            )
            # Append only non-empty, non-all-NA DataFrames
            if not query_df.empty and not query_df.isna().all().all():
                valid_dataframes.append(query_df)

        # If in epoch mode and there are valid DataFrames, concatenate and save
        if self.epoch_mode and valid_dataframes:
            query_list_df = pd.concat(valid_dataframes, ignore_index=True)
            self.__save_csv(query_list_df, "data/posts/epochs", str(self.epoch_num))

    def __extract_csv_name(self, filepath):
        """
        Extracts the name of a CSV file from a filepath string, without the '.csv' extension or the rest of the filepath.
        
        Parameters:
            filepath (str): The full filepath of the CSV file.
            
        Returns:
            str: The name of the CSV file without the extension.
        """
        # Regular expression to extract the filename without extension
        match = re.search(r'([^/\\]+)\.csv$', filepath)
        if match:
            return match.group(1)
        return None

    def __get_start_from_epoch(self, epoch):
        """
        Returns the start_id based on the given epoch, using scientific notation.

        The value of start_id for each epoch is defined as follows:
        Epoch 0: 0
        1: 100000000000000000
        2: 101000000000000000
        3: 102000000000000000
        4: 103000000000000000
        etc.

        Parameters:
            epoch (int): The epoch number.

        Returns:
            tuple: A tuple containing (start_id, end_id).
        """
        if epoch == 0:
            return 0

        base = 1e17
        increment = 1e15
        start_id = base + (epoch - 1) * increment

        return start_id

    def __run_epoch(self, num_posts_per_query):
        """
        Runs a single epoch.
        An epoch simply means a period of time.
        In this case, an epoch consists of Mastodon posts matching every search query in query_list.
        
        Can load a checkpoint from the existing filenames in data/posts/epochs.
        If none are present, starts from epoch 0.

        Parameters:
        - num_posts_per_query (int): Number of posts to fetch per query.
            Default is 20 posts per query. Max is 40 posts per query.
        """

        # Construct the file path pattern
        file_pattern = os.path.join("data/posts/epochs", "*.csv")
        
        # Get a list of all CSV files in the directory
        csv_files = glob.glob(file_pattern)

        # Find the numerical names of files in the directory
        csv_names = [self.__extract_csv_name(filepath) for filepath in csv_files]
        csv_names = [name for name in csv_names if name.isdigit()]
        csv_names.sort()

        # Determine which epoch to start from
        if csv_names:
            self.epoch_num = (int)(csv_names[-1]) + 1
        else:
            self.epoch_num = 0

        start_id = self.__get_start_from_epoch(self.epoch_num)

        print(f"Running epoch {self.epoch_num}...")

        self.search_list_of_queries(num_posts_per_query=num_posts_per_query,
                                    start_id=start_id)

    def run_epochs(self, num_epochs, num_posts_per_query=20):
        """
        Runs several epochs at once.

        Parameters:
        - num_posts_per_query (int): Number of posts to fetch per query.
            Default is 20 posts per query. Max is 40 posts per query.
        """
        self.epoch_mode = True
        for i in range(num_epochs):
            self.__run_epoch(num_posts_per_query)
        self.epoch_mode = False

    def combine_epochs(self, directory="data/posts/epochs", id_column="id"):
        """
        Combines all CSV files in the specified directory, removes duplicates by the specified column,
        and sorts the resulting DataFrame by that column.

        Parameters:
            directory (str): The directory path containing the CSV files.
            id_column (str): The column name used for duplicate removal and sorting.

        Returns:
            pd.DataFrame: The combined, cleaned, and sorted DataFrame.
        """

        # Construct the file path pattern
        file_pattern = os.path.join(directory, "*.csv")
        
        # Get a list of all CSV files in the directory
        csv_files = glob.glob(file_pattern)

        print(f"csv_files: {csv_files}")
        
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in the directory: {directory}")
        
        # Load DataFrames and filter out empty or all-NA DataFrames
        dataframes = []
        for file in csv_files:
            df = pd.read_csv(file)
            if not df.empty and not df.isna().all().all():
                dataframes.append(df)
        
        if not dataframes:
            raise ValueError("No non-empty or non-all-NA CSV files to combine.")
        
        # Combine the filtered DataFrames
        combined_df = pd.concat(dataframes, ignore_index=True)
        
        # Remove duplicates by the specified column
        combined_df = combined_df.drop_duplicates(subset=id_column)
        
        # Sort the DataFrame by the specified column
        combined_df = combined_df.sort_values(by=id_column).reset_index(drop=True)
        
        # Save the combined DataFrame
        self.__save_csv(combined_df, "data/posts/epochs", "combined_epochs")

        return combined_df


# Example of class usage.
# Place your own Mastodon credentials here.
mastodon = Mastodon(
    client_id='',
    client_secret='',
    access_token=''
)

#mastodon.get_list_of_queries("query_list.txt")
#mastodon.run_epochs(10, num_posts_per_query=40)
mastodon.combine_epochs()