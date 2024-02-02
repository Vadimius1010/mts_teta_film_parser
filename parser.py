import time

from tqdm import tqdm
import pandas as pd
import requests
from bs4 import BeautifulSoup

from multiprocessing import Pool, cpu_count


def parse(year):
    out = []
    pages = requests.get(f"https://www.kinoafisha.info/rating/movies/{year}/")
    soup = BeautifulSoup(pages.text, "html.parser")

    time.sleep(1)

    all_films = soup.findAll("div", class_="movieItem_info")
    for film in all_films:
        film_name = film.find("a", class_="movieItem_title").text
        genre = film.find("span", class_="movieItem_genres").text
        _, country = film.find("span", class_="movieItem_year").text.split(", ")
        out.append([film_name, genre, year, country])
    return out


years = range(1935, 2025)
data = []
with Pool(processes=cpu_count()) as pool, tqdm(years) as pbar:
    for info in pool.imap_unordered(parse, list(years)):
        data.extend(info)
        pbar.update()
header = ("film_name", "genre", "year", "country")
data_file = pd.DataFrame(data, columns=header)
data_file.to_csv("films.csv", sep=";", encoding="utf8")
