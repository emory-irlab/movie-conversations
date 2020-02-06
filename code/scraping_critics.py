import requests
from requests import TooManyRedirects
import time
from bs4 import BeautifulSoup
import json
import traceback
import pandas as pd
import numpy as np
import os

'''
    Author: Sergey Volokhin (github: sergey-volokhin)

    This code scrapes Critics reviews from RottenTomatoes website
    for movies, which links are in "data/films_links.json" file.
'''


datapath = 'tmp'

# mapping of most common critics scores to [1; 5] scale
score_map = {'*****': 5,
             '****': 4,
             '***': 3,
             '**': 2,
             '*': 1,
             'A-PLUS': 5,
             'A PLUS': 5,
             'A+': 5,
             'A': 5,
             'A-': 5,
             'A -': 5,
             'A MINUS': 5,
             'A-MINUS': 5,
             'B PLUS': 4,
             'B-PLUS': 4,
             'B +': 4,
             'B+': 4,
             'B': 4,
             'B-': 4,
             'B MINUS': 4,
             'B-MINUS': 4,
             'C PLUS': 3,
             'C-PLUS': 3,
             'C+': 3,
             'C': 3,
             'C-': 3,
             'C-MINUS': 3,
             'C MINUS': 3,
             'D+': 2,
             'D PLUS': 2,
             'D': 2,
             'D-': 2,
             'E+': 1,
             'E': 1,
             'E-': 1,
             'F+': 1,
             'F': 1,
             'F-': 1,
             }


# converting the critics score to [1; 5] scale
def calculate_score(score):
    if score is None or score.strip() == '':
        return np.nan
    score = score.strip().replace('  ', ' ')
    try:
        res = float(eval(score.replace('\'', '').replace('"', '').replace(' stars out of ', '/').replace(' stars', '/5').replace(' out of ', '/').replace(' of ', '/')))
        if 0 <= res <= 1:
            return max(1, round(res*5))
        return np.nan
    except Exception:
        try:
            return round(score_map[score.upper()])
        except Exception:
            return np.nan


# return soup of the page, after waiting for $crawl_rate$ seconds (to not get banned)
def make_soup(url, crawl_rate=1):
    time.sleep(crawl_rate)
    try:
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html.parser')
    except TooManyRedirects:
        soup = ''
    return soup


def get_critics_from_movie(movie):
    ''' Scrapes all ids from critics, who left reviews about movie "movie" '''

    soup = make_soup(f"https://www.rottentomatoes.com/m/{movie}/reviews")

    try:
        page_nums = int(soup.find("span", class_='pageInfo').text[9:])
    except AttributeError:
        page_nums = 1

    critics = []
    for page_num in range(1, page_nums+1):
        page_soup = make_soup(f"https://www.rottentomatoes.com/m/{movie}/reviews?page={page_num}&sort=")
        reviews_soups = page_soup.find_all("div", class_="row review_table_row")
        for review_soup in reviews_soups:
            try:
                critics += [review_soup.find('a', class_='unstyled bold articleLink')['href'][8:]]
            except Exception:
                pass
    return critics


def get_reviews_from_critic(critic):
    '''
        Scrapes all reviews left by critic "critic".
        Includes critic_id, movie_id, text of the review, score, and "freshness" of the movie.
        Writes excepted critics ids into "datapath/failed_critics.txt"
    '''
    try:
        page = requests.get(f'https://www.rottentomatoes.com/napi/critic/{critic}/review/movie?offset=0').json()

        all_reviews_f_critic = []
        offset = 0
        total = page['totalCount']
        while offset < total:
            time.sleep(1)  # to not get banned
            page = requests.get(f'https://www.rottentomatoes.com/napi/critic/{critic}/review/movie?offset={offset}').json()
            for review in page['results']:
                current_review = {'critic_id': critic}
                try:
                    current_review['movie_id'] = review['media']['url'][33:].replace('-', '_')
                except Exception:
                    continue
                current_review['fresh'] = review['score']
                current_review['score'] = review['scoreOri']
                current_review['review'] = review['quote']
                all_reviews_f_critic.append(current_review)
            offset += len(page['results'])
        return all_reviews_f_critic

    except Exception as err:
        print(f"couldn't get reviews for {critic}. {err}")
        traceback.print_exc()
        open(datapath+'failed_critics.txt', 'a+').write(critic+'\n')
        return []


def get_reviews_from_movie(page):
    '''
        Function not currently used.
        Scrapes all reviews for movie "movie".
    '''

    soup = make_soup(f"https://www.rottentomatoes.com/m/{page}/reviews")

    # getting the amount of pages of reviews
    try:
        page_nums = int(soup.find("span", class_='pageInfo').text[9:])
    except AttributeError:
        page_nums = 1

    reviews = []
    for page_num in range(1, page_nums+1):
        page_soup = make_soup(f"https://www.rottentomatoes.com/m/{page}/reviews?page={page_num}&sort=")
        reviews_soups = page_soup.find_all("div", class_="row review_table_row")
        for review_soup in reviews_soups:
            cur_review = {}
            cur_review['movie_id'] = page.replace('-', '_')
            try:
                cur_review['critic_id'] = review_soup.find('a', class_='unstyled bold articleLink')['href'][8:]
            except Exception:
                continue

            # getting text
            cur_review['review'] = review_soup.find('div', class_='the_review').text.strip()

            # getting freshness
            if review_soup.find('div', class_='review_icon icon small fresh') is not None:
                cur_review['fresh'] = 'fresh'
            else:
                cur_review['fresh'] = 'rotten'

            # getting score
            try:
                cur_review['score'] = review_soup.find('div', class_='small subtle review-link').text.split('Original Score: ')[1].split('\n')[0]
            except Exception:
                cur_review['score'] = ''

            reviews.append(cur_review)
    return reviews


if __name__ == '__main__':

    movies = json.load(open('../data/films_links.json', 'r'))

    if not os.exists(datapath):
        os.system('mkdir ' + datapath)

    critics = []
    for film in movies:
        new_critics = get_critics_from_movie(film)
        print(f'GOT {len(new_critics)} CRITICS FROM', film)
        critics += new_critics

    critics = sorted(list(set(critics)))
    open(datapath+'critics_list.txt', 'w').write('\n'.join(critics))

    print('Critics collection finished')
    print("\n============================================\n")

    critics = open(datapath+'critics_list.txt', 'r').read().split('\n')

    # reviews = json.load(open(datapath+'reviews.json', 'r'))
    reviews = []
    open(datapath+'failed_critics.txt', 'w').close()

    print(f'GETTING REVIEWS FROM {len(critics)} CRITICS')
    for critic in critics:
        if critic != '':
            reviews += get_reviews_from_critic(critic)

    failed_critics = open(datapath+'failed_critics.txt', 'r').read().split('\n')
    open(datapath+'failed_critics.txt', 'a+').write('\n')
    print(f'RE-GETTING REVIEWS FROM {len(failed_critics)} FAILED CRITICS')
    for critic in failed_critics:
        if critic != '':
            reviews += get_reviews_from_critic(critic)

    df_all_reviews = pd.DataFrame.from_records(reviews).dropna()
    df_all_reviews['score'] = df_all_reviews['score'].apply(calculate_score)
    df_all_reviews = df_all_reviews.replace('', np.nan).dropna()

    # cleanup of common templates
    df_all_reviews = df_all_reviews[df_all_reviews['movie_id'] != ':vanity']
    df_all_reviews['review_lower'] = df_all_reviews['review'].apply(lambda x: str(x).lower())
    df_all_reviews = df_all_reviews[~df_all_reviews['review_lower'].isin(['see website for more details.', '.'])]
    df_all_reviews = df_all_reviews[~df_all_reviews['review_lower'].str.startswith('click to ')]
    df_all_reviews = df_all_reviews[~df_all_reviews['review_lower'].str.startswith('click for ')]
    df_all_reviews = df_all_reviews[~df_all_reviews['review_lower'].str.startswith('full review ')]
    df_all_reviews.drop('review_lower', axis=1).to_csv(datapath+'reviews_unclipped.tsv', sep='\t', index=False)

    print('Reviews collection finished')
    print('TOTAL CRITICS:', df_all_reviews['critic_id'].nunique())
    print("TOTAL REVIEWS:", df_all_reviews.shape[0])

    groupped = df_all_reviews.groupby('critic_id')
    print('  amount of movies:', len(df_all_reviews.groupby('movie_id')))
    print('  median of reviews per critic:', np.median([len(i[1]) for i in groupped]))
    print('  mean of reviews per critic:', np.mean([len(i[1]) for i in groupped]))

    zero_reviewed = [movie_id for movie_id in movies if movie_id not in df_all_reviews['movie_id']]
    print(len(zero_reviewed), 'movies with 0 reviews:')
    print('\n   '.join(zero_reviewed))
