import os
import re
from bs4 import BeautifulSoup
from core.crawler import Crawler


class GlosbeStatsCrawler(Crawler):
    """
    Crawl statistics of language pairs from Glosbe.
    """

    @property
    def base_url(self):
        return 'https://glosbe.com'

    @staticmethod
    def prepare(context, args):
        html = "".join(open('crawlers/glosbe/glosbe_en.html', encoding='utf-8').readlines())
        soup = BeautifulSoup(html, 'html.parser')
        lng_set = set()
        for lng in soup.select('.row .span5 li a'):
            src, tgt, _ = GlosbeStatsCrawler._get_lang(lng['href'])
            lng_set.add(src)
            lng_set.add(tgt)
        lng_set = list(lng_set)
        urls = []
        for lng1 in lng_set:
            for lng2 in lng_set:
                if lng1 != lng2:
                    urls.append("/%s/%s" % (lng1, lng2))

        if os.path.exists('outputs/stats.csv'):
            context['file'] = open('outputs/stats.csv', 'a', encoding='utf-8')
        else:
            context['file'] = open('outputs/stats.csv', 'a', encoding='utf-8')
            context['file'].write("src,tgt,#dict,#phrase\n")
            args['restart'] = True
        return urls

    def parse(self, soup, url):
        src_lang, tgt_lang, langs = self._get_lang(url)
        try:
            stats = soup.select(".dictionaryWelcomePage p")[4].text.strip()
            stats = re.findall("[\d,]+", stats)
            self.add_result(
                "%s,%s,%s,%s\n" % (src_lang, tgt_lang, stats[0].replace(",", ""), stats[1].replace(",", "")))
        except:
            print(soup.select(".dictionaryWelcomePage p"), url, soup)

    @staticmethod
    def collect_results(context, result):
        context['file'].write(result)

    def clean_url(self, url):
        return self._clean_url(url)

    @staticmethod
    def _clean_url(url):
        url = url.replace("https://glosbe.com", "")
        url = url.strip()
        url = url.rstrip('/')
        url = url.replace(r"&tmmode=MUST", "")
        return url

    @staticmethod
    def _get_lang(url):
        url = GlosbeStatsCrawler._clean_url(url)
        url_splits = [w for w in url.split("/") if w != ""]
        src_lang = url_splits[0]
        tgt_lang = url_splits[1].split('?')[0]
        return src_lang, tgt_lang, "%s_%s" % (src_lang, tgt_lang)


if __name__ == "__main__":
    GlosbeStatsCrawler.start(
        task_name="glosbe_stats",
        proxy_pool='mixed',
        thread_num=1000,
        restart=True
    )
