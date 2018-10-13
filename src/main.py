import bs4
import psycopg2
import random
import requests


def soup(url):
    text = requests.get(url).text
    return bs4.BeautifulSoup(text, 'html.parser')


def max_pages():
    url = 'https://dbase.tube/chart/channels/subscribers/all'
    s = soup(url)

    button = s.select('a[href^="/chart/channels/subscribers/all?page="]')[1]
    page_count = int(button['href'].split('=')[-1])
    return page_count


def channels(idx):
    def page():
        url = f'https://dbase.tube/chart/channels/subscribers/all?page={idx}'
        if idx == 1:
            url = 'https://dbase.tube/chart/channels/subscribers/all'

        text = requests.get(url).text
        return bs4.BeautifulSoup(text, 'html.parser')

    s = page()
    tags = s.select('a[href^="/c/"]')

    chans = []
    for c in tags:
        href = c['href'].split('/')[-2]
        chans.append(href)

    return chans


def connect():
    return psycopg2.connect(user='admin', password='admin', host='192.168.1.63', port='5432', database='youtube')


def insert(conn, serial):
    sql = 'INSERT INTO youtube.simple.channels (chan_serial) VALUES (%s) ON CONFLICT (chan_serial) DO NOTHING;'
    cursor = conn.cursor()

    cursor.execute(sql, [serial])
    conn.commit()
    cursor.close()


def main():
    while True:
        conn = connect()
        pages = max_pages()
        range_nums = list(range(1, pages + 1))
        random.shuffle(range_nums)

        print('Found', pages, 'pages')

        for i in range_nums:
            print('On page', i)

            chans = channels(i)
            print('Found', len(chans))
            for c in chans:
                print('Inserting', c)
                insert(conn, c)

        conn.close()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Detected Ctrl-C - Quitting...')