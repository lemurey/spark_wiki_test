from bs4 import BeautifulSoup
from tqdm import tqdm
import requests
import os
from multiprocessing import Pool

base_url = 'https://dumps.wikimedia.org/other/pagecounts-raw/{year}/{year}-{:0>2}'

def get_filenames(month, year=2016):
    url = base_url.format(month, year=year)
    raw = requests.get(url).content
    soup = BeautifulSoup(raw, 'html.parser')
    ul = soup.find('ul')
    elements = ul.find_all('li')
    urls = []
    for i, entry in enumerate(elements):
        if i < 3:
            continue
        file_name = entry.find(href=True)['href']
        urls.append(url+'/{}'.format(file_name))

    return urls

def download_file(url):
    response = requests.get(url, stream=True)
    file_name = url.split('/')[-1]
    with open(file_name, 'wb') as handle:
        for data in tqdm(response.iter_content()):
            handle.write(data)


def get_files(month, year=2016):
    
    url = base_url.format(month, year=year)
    raw = requests.get(url).content
    soup = BeautifulSoup(raw, 'html.parser')
    ul = soup.find('ul')
    elements = ul.find_all('li')
    print len(elements)
    for i, entry in enumerate(elements):
        if i == 0:
            continue
        file_name = entry.find(href=True)['href']
        # response = requests.get(url+'/{}'.format(file_name), stream=True)
        # print file_name
        # with open(file_name, "wb") as handle:
        #     for data in tqdm(response.iter_content()):
        #         handle.write(data)

        # bucket.set_contents_from_filename(file_name)
        # os.remove(file_name)
    print url

def main():
    month = 2
    urls = get_filenames(month)
    for url in urls:
        print url
    # pool = Pool(40)
    # pool.map(download_file, urls)


if __name__ == '__main__':
    main()



