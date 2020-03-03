import re
import requests
import random
import string
import time
import bs4 as BeautifulSoup


waittimes = 0


def start():
    dataPath = r'books2.txt'
    outpath = r'booksout.txt'
    out = open(outpath, 'w')

    with open(dataPath, 'r') as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip('\n')
            l = line.split("\t")
            isbn = l[4]
            genre = getGenre(isbn)
            outs = line+'\t'+genre+'\n'
            print(outs)
            out.write(outs)
    out.close()

def getGenre(ISBN):
    urlpre = r'https://www.alibris.com/booksearch?keyword='
    #pattern = re.compile(r'red\/home.gif.+?<li>.+?<li>\s+?<a.+?>(.+?)<\/a>')
    pattern = re.compile(r'red\/home.gif.+?<li>.+?<li>\s+?<a.+?>(.+?)<\/a>')
    #psub1 = re.compile(r'red.+">')
    #psub2 = re.compile(r'</a>')
    highvolpattern = re.compile(r'due to the high volume of visitors')
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; WOW64; MSIE 10.0; Windows NT 6.2)',
        'From': ''  # This is another valid field
    }
    UA = ['Mozilla/5.0 (Windows NT 10.0; WOW64; rv:56.0) Gecko/20100101 Firefox/56.0',
          'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.9 Safari/537.36',
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36 OPR/48.0.2685.52',
          'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:56.0) Gecko/20100101 Firefox/56.0',
          'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
          'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063',
          'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
          'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.49 Safari/537.36 OPR/48.0.2685.7',
          'Mozilla/5.0 (X11; Linux i586; rv:31.0) Gecko/20100101 Firefox/31.0',
          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.75 Safari/537.36',
          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36 OPR/47.0.2631.55',
          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:56.0) Gecko/20100101 Firefox/56.0',
          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/604.3.5 (KHTML, like Gecko) Version/11.0.1 Safari/604.3.5']

    headersfrom = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(5))
    headers['From'] = headersfrom + "@alibris.com"
    url = urlpre + ISBN + r'&mtype=B'
    html = requests.get(url, headers=headers)
    text = html.text
    soup = BeautifulSoup.BeautifulSoup(html.text,features="lxml")
    match = pattern.search(text)
    global waittimes
    if match:
        bookpath = soup.find('ul', class_="path")
        genreraw = ''
        print(bookpath)

        array = re.split("<\/a>",str(bookpath))
        subject = []
        for stringa in array:
            if(re.search("<a href=\"\/search/books/subject/",stringa)):
                subject.append(re.sub(".*\">","",stringa))

        print(subject)

        for tags in bookpath.strings:
            genreraw = genreraw + tags + '\n'

        genre = re.subn(r'(\n\s)+|(\s\n)+|(\n+)|(ISBN.*)|(,\s)+|(\s,)+|Books', ',', genreraw)[0]
        #genre = re.subn(r'(^[\s,]+)|[\s,]+|(ISBN.*)|Books', ',', genreraw)[0]
        genre = re.subn(r'(^,+)|,+$', '', genre)[0]
        genre = re.subn(r',+', ',', genre)[0]
        genre = re.subn(r'Subject:,', '', genre)[0]# for subject:,
        genre = re.subn(r' > ', ',', genre)[0]
        genre = re.subn(r'FICTION', 'Fiction', genre)[0]  #,([a-zA-Z0-9:()]+\s?){3,}$
        genre = re.subn(r',([a-zA-Z0-9:()\']+\s?){3,}$', '', genre)[0]
        genre = re.subn(r' - .*$', '', genre)[0]
        genre = re.subn(r'/', ',', genre)[0]
        waittimes = 0
        if genre == '':
            return 'NA'
        else:
            return genre
    else:
        highvol = highvolpattern.search(text)

        if highvol:
            if waittimes >= 3:
                print('long wait... INNER PEACE...')
                for s in range(1, 60):
                    print(s)
                    time.sleep(1)
                waittimes = 0
            print('wait')
            time.sleep(1)
            waittimes += 1
            print('wait tiems = '+ str(waittimes) + '\n')
            headers['User-Agent'] = UA[random.randint(0, 13)]
            res = getGenre(ISBN)
            return res
        else:
            waittimes = 0
            return 'NA'


if __name__ == "__main__":
    #start()
    print(getGenre('978043902348'))

    #url = urlpre + ISBN + r'&mtype=B'
    #html = requests.get(url, headers=headers)
    #text = html.text
    #soup = BeautifulSoup(html.text)
    #bookpath = soup.find('ul', class_="path")
    # g = ''
    #for s in bookpath.strings:
    #   g = g + s + '\n'
    #res = re.subn(r'(\n\s)+|(\s\n)+|(\n+)|(ISBN.*)|(,\s)+|(\s,)+|Books', ',', g)[0]
    #res = re.subn(r'(^,+)|,+$','',res)[0]