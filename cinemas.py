import re
import requests
from datetime import date
from multiprocessing.dummy import Pool as ThreadPool

from bs4 import BeautifulSoup

KINOPOISK_URL = 'https://www.kinopoisk.ru/index.php'
MOVIES_COUNT = 10
CINEMAS_MIN_COUNT = 20
POOL_SIZE = 20


def fetch_afisha_page():
    r = requests.get('http://www.afisha.ru/msk/schedule_cinema/')
    if r.status_code == requests.codes.ok:
        return r.content


def parse_afisha_list(raw_html):
    soup = BeautifulSoup(raw_html, 'html.parser')
    return [
        (
            movie.select('.usetags a')[0].text,
            len(movie.select('table tbody tr'))
        )
        for movie in soup.select('.object')
    ]


def fetch_movie_info(movie):
    movie_title, cinemas_count = movie
    current_year = date.today().year
    payload = {
        'first': 'yes',
        'kp_query': '{} {}'.format(movie_title, current_year),
    }
    r = requests.get(KINOPOISK_URL, params=payload)

    soup = BeautifulSoup(r.content, 'html.parser')
    rating = soup.select('.rating_ball')
    if rating:
        rating = float(rating[0].text)

    rating_count = soup.select('.ratingCount')
    if rating_count:
        rating_count = ''.join(re.findall('[0-9]+', rating_count[0].text))

    return {
        'title': movie_title,
        'cinemas_count': cinemas_count,
        'rating': rating or 0,
        'rating_count': rating_count or 0,
    }


def update_movies_info(movies):
    pool = ThreadPool(POOL_SIZE)
    res = pool.map(lambda x: fetch_movie_info(x), movies)
    pool.close()
    pool.join()
    return res


def output_movies_to_console(movies):
    movies.sort(key=lambda x: x['rating'], reverse=True)
    movies = list(filter(lambda x: x['cinemas_count'] > CINEMAS_MIN_COUNT, movies))
    for movie in movies[:MOVIES_COUNT]:
        print('{} ({}) in {} cinemas'.format(
            movie['title'],
            movie['rating'],
            movie['cinemas_count'],
        ))


if __name__ == '__main__':
    html = fetch_afisha_page()
    if html is not None:
        movies = parse_afisha_list(html)
        movies = update_movies_info(movies)
        output_movies_to_console(movies)
