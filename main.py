import requests
import time
import csv
import random
import concurrent.futures


from bs4 import BeautifulSoup

# Global headers to be used for requests
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}

MAX_THREADS = 10


def extract_movie_details(movie_link):
    time.sleep(random.uniform(0, 0.2))
    response = BeautifulSoup(requests.get(movie_link, headers=headers).content, 'html.parser')
    movie_soup = response

    if movie_soup is not None:
        title = None
        date = None

        movie_data = movie_soup.find('div', attrs={'class': 'sc-1f50b7c-0 PUxFE'})
        if movie_data is not None:
            title = movie_data.find('span').get_text()
            date = movie_data.find('a', attrs={'class': 'ipc-link ipc-link--baseAlt ipc-link--inherit-color'}).get_text().strip()

        rating = movie_soup.find('span', attrs={'class': 'sc-eb51e184-1 cxhhrI'}).get_text() if movie_soup.find(
            'span', attrs={'class': 'sc-eb51e184-1 cxhhrI'}) else None

        plot_text = movie_soup.find('span', attrs={'class': 'sc-96357b74-2 CKcbM'}).get_text().strip() if movie_soup.find(
            'span', attrs={'class': 'sc-96357b74-2 CKcbM'}) else None

        with open('movies.csv', mode='a', newline='') as file:
            movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            if all([title, date, rating, plot_text]):
                print(title, date, rating, plot_text)
                movie_writer.writerow([title, date, rating, plot_text])


def extract_movies(soup):

    movie_raw_links = []

    for i, movie in enumerate(soup.find_all('a', attrs={'class': 'ipc-title-link-wrapper'})):
        if i < 100:
            movie_raw_links.append(movie['href'])
        else:
            break
            print(i)

    movie_links = ['https://imdb.com' + movie for movie in movie_raw_links]

    threads = min(MAX_THREADS, len(movie_links))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(extract_movie_details, movie_links)


def main():
    start_time = time.time()

    # IMDB Most Popular Movies - 100 movies
    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    response = requests.get(popular_movies_url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Main function to extract the 100 movies from IMDB Most Popular Movies
    extract_movies(soup)

    end_time = time.time()
    print('Total time taken: ', end_time - start_time)


if __name__ == '__main__':
    main()