import requests
import traceback
from pprint import pprint
import inspect
import csv
import pandas as pd


POSTS_BASE_URL = 'https://www.techinasia.com/wp-json/techinasia/2.0/'
# POSTS_BASE_URL = 'https://www.techinasia.com/wp-json/techinasia/2.0/posts' # obtain full list of posts
# COMMENTS_BASE_URL = 'https://www.techinasia.com/wp-json/techinasia/2.0/posts/%7Bid%7D/comments' # obtain comments associated to each post
JOBS_BASE_URL = 'https://www.techinasia.com/api/2.0/job-postings' # obtain list of jobs that are currently active
POSTS_CSV = 'TIA_posts.csv'
POSTS_COMMENTS_CSV = 'TIA_posts_comments.csv'

HEADERS = {
    'httpAccept':'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0'
}

# =============================================================================
#                                Main scraping methods
# =============================================================================

def get_all_posts():
    global POSTS_CSV
    page_num = 1
    total_pages = 1 # should ideally be replaced after first call to API
    # total_pages = 10 # for testing, will comment out later

    with open(POSTS_CSV, 'w+') as f:
        # writer = csv.writer(f, delimiter=',')
        writer = csv.DictWriter(f, fieldnames = ['id', 'page_number', 'date_gmt', 'modified_gmt', 'title', 'slug', 'status', 'type', 'link', 'content', 'excerpt', 'categories', 'tags', 'read_time'])
        writer.writeheader()

        # end process if page limit is reached
        while True and page_num <= total_pages:
            page_data = get_page_posts(page_num)

            print('CURRENT PAGE: {}'.format(page_num))
            print('total pages: {}'.format(total_pages))
            # if available, store page data to CSV
            if page_data:
                store_posts_page_data(page_data, writer, page_num)
            print()

            # replace total pages # **comment out for testing
            if page_num == 1:
                total_pages = int(page_data['total_pages'])

            page_num += 1


def get_all_post_comments():
    global POSTS_CSV
    global POSTS_COMMENTS_CSV
    post_ids = collect_post_ids(POSTS_CSV)

    with open(POSTS_COMMENTS_CSV, 'w+') as f:
        # writer = csv.writer(f, delimiter=',')
        writer = csv.DictWriter(f, fieldnames = ['post_id', 'id', 'post_title', 'post_link', 'content', 'excerpt', 'author_id', 'author_display_name', 'parent', 'upvotes'])
        writer.writeheader()
        for post_id in post_ids:
            post_comments = get_post_comments(post_id)
            store_post_comments(post_comments, writer, post_id)

# =============================================================================
#                                Wrapper Functions 
# =============================================================================

def get_page_posts(page_num):
    global POSTS_BASE_URL
    params = {
        'page':'{}'.format(page_num)
    }
    query_string = POSTS_BASE_URL + 'posts?'
    return _request_api(query_string, params)


def get_post_comments(post_id):
    global POSTS_BASE_URL
    query_string = POSTS_BASE_URL + 'posts/{}/comments'.format(post_id)
    return _request_api(query_string, {})

# =============================================================================
#                                Base Callers 
# =============================================================================

def _request_api(query_string, params):
    global HEADERS

    try:
        req = requests.get(query_string, params = params, headers = HEADERS)
        if req.status_code == 200:
            print('HTTP [200] OK.')
            return req.json()
        else:
            req.raise_for_status()
        return req
    except:
        traceback.print_exc()
        return {}

# =============================================================================
#                                Data Storage 
# =============================================================================

# store data collected in each posts page in CSV
def store_posts_page_data(page_data, writer, page_num):
    try:
        posts = page_data['posts']

        for post in posts:
            try:
                # deliberately left out certain features (e.g. author, image) due to lack of time
                data = {
                    'id': post['id'], 
                    'page_number': page_num,
                    'date_gmt': post['date_gmt'], 
                    'modified_gmt': post['modified_gmt'],
                    'title': post['title'], 
                    'slug': post['slug'], 
                    'status': post['status'], 
                    'type': post['type'], 
                    'link': post['link'], 
                    'content': post['content'], 
                    'excerpt': post['excerpt'], 
                    'categories': parse_data_dict(post['categories']), 
                    'tags': parse_data_dict(post['tags']), 
                    'read_time': post['read_time']
                }
                # pprint(data)
                writer.writerow(data)

            except:
                try:
                    print('ERROR IN POST. ID: {}'.format(post['id']))
                except:
                    traceback.print_exc()
                traceback.print_exc()

    except:
        try:
            print('ERROR IN PAGE. PAGE NUMBER: {}'.format(page_data['current_page']))
        except:
            traceback.print_exc()
        traceback.print_exc()


# store comments for each post in CSV
def store_post_comments(post_comments, writer, post_id):
    try:
        comments = post_comments['comments']
        if comments:
            for comment in comments:
                try:
                    write_comment_data(post_id, comment, writer)
                    # print('1: {}'.format(post_id))
                    if 'replies' in comment:
                        replies = comment['replies']
                        if replies:
                            for reply in replies:
                                write_comment_data(post_id, reply, writer)
                                # print('2: {}'.format(post_id))

                except:
                    try:
                        print('ERROR IN COMMENT. ID: {}'.format(comment['id']))
                    except:
                        traceback.print_exc()
                    traceback.print_exc()

    except:
        try:
            print('ERROR IN POST. ID: {}'.format(post_id))
        except:
            traceback.print_exc()
        traceback.print_exc()

# =============================================================================
#                                Helpers
# =============================================================================

def parse_data_dict(data_dict):
    output = []
    for dat in data_dict:
        output.append(dat['name'])
    return ','.join(output)


# read CSV with posts, collect comments
def collect_post_ids(posts_csv):
    df = pd.read_csv(posts_csv)
    return list(df['id'].unique())


def write_comment_data(post_id, comment, writer):
    data = {
        'post_id': post_id,
        'id': comment['id'],
        'post_title': comment['post_title'],
        'post_link': comment['post_link'],
        'content': comment['content'],
        'excerpt': comment['excerpt'],
        'author_id': comment['author']['id'],
        'author_display_name': comment['author']['display_name'],
        'parent': comment['parent'],
        'upvotes': comment['upvotes']
    }
    writer.writerow(data)


# =============================================================================
#                                Main Method
# =============================================================================

if __name__ == "__main__": 
    get_all_posts()
    get_all_post_comments()