import bs4
import psycopg2
import random
import requests


def soup(url):
    text = requests.get(url).text
    return bs4.BeautifulSoup(text, 'html.parser')


def max_pages():
    url = 'https://dbase.tube/chart/video/views/all'
    s = soup(url)

    button = s.select('a[href^="/chart/video/views/all?page="]')[1]
    page_count = int(button['href'].split('=')[-1])
    return page_count


def videos(idx):
    def page():
        url = f'https://dbase.tube/chart/video/views/all?page={idx}'
        if idx == 1:
            url = 'https://dbase.tube/chart/video/views/all'

        text = requests.get(url).text
        return bs4.BeautifulSoup(text, 'html.parser')

    s = page()
    tags = s.select('a[href^="/v/"]')

    chans = []
    for c in tags:
        href = c['href'].split('/')[-1]
        chans.append(href)

    return chans


def connect():
    return psycopg2.connect(user='admin', password='admin', host='192.168.1.63', port='5432', database='youtube')


def insert(conn, video_serial, chan_serial):
    cursor = conn.cursor()

    def query_channel():
        sql_query = 'SELECT id FROM youtube.simple.channels WHERE chan_serial = %s LIMIT 1'
        cursor.execute(sql_query, [chan_serial])
        chan = cursor.fetchone()
        conn.commit()

        return chan[0]

    def insert_channel():
        sql = 'INSERT INTO youtube.simple.channels (chan_serial) VALUES (%s) ON CONFLICT (chan_serial) DO NOTHING;'
        cursor = conn.cursor()

        cursor.execute(sql, [chan_serial])
        conn.commit()

    insert_channel()
    chan_id = query_channel()

    sql_insert = 'INSERT INTO youtube.simple.videos (chan_id, video_serial) VALUES (%s, %s) ON CONFLICT (video_serial) DO NOTHING;'
    cursor.execute(sql_insert, [chan_id, video_serial])
    conn.commit()
    cursor.close()


def chan_serial_from_vid_serial(vid_serial):
    url = f'https://dbase.tube/v/{vid_serial}'
    s = soup(url)

    def find_channel():
        return s.find('div', id='video_page_header').select_one('a[href^="/c/"]')['href'].split('/')[-1]

    chan_serial = find_channel()
    return chan_serial


def main():
    while True:
        conn = connect()
        pages = max_pages()
        range_nums = list(range(1, pages + 1))
        random.shuffle(range_nums)

        print('Found', pages, 'pages')

        for i in range_nums:
            print('On page', i)

            vids = videos(i)
            print('Found', len(vids))
            for v in vids:
                print('Inserting', v)
                chan_serial = chan_serial_from_vid_serial(v)
                print('Video', v, 'belongs to channel', chan_serial)
                insert(conn, v, chan_serial)

        conn.close()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Detected Ctrl-C - Quitting...')