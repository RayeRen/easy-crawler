import json
import os
import random
import re
import yaml
from core.crawler import Crawler


class DictCrawler(Crawler):
    """
    Crawl dictionaries of language pairs from Glosbe.
    """

    @property
    def base_url(self):
        return 'https://glosbe.com'

    @staticmethod
    def prepare(context, args):
        src = args['src']
        tgt = args['tgt']
        output_dir = args['output_dir']
        fn, dict_fn, phrase_fn = DictCrawler.make_fn(src, tgt)
        if args.get('restart', False):
            try:
                os.remove(output_dir + '/' + phrase_fn)
                os.remove(output_dir + '/' + dict_fn)
            except:
                pass
        os.makedirs(output_dir, exist_ok=True)
        context['files'] = {
            dict_fn: open(output_dir + "/" + dict_fn, 'a', encoding='utf-8'),
            phrase_fn: open(output_dir + "/" + phrase_fn, 'a', encoding='utf-8')
        }

        context['unique_phrases'] = set()

        base = '/%s/%s/' % (src, tgt)
        start_list = [base]
        if args['seed_list'] is not None:
            start_list += [base + u for u in random.sample(args['seed_list'], args['seed_num'])]
        return start_list

    def parse(self, soup, url):
        new_urls = [t['href'] for t in soup.select("#wordListContainer li a")]
        nav = None
        try:
            nav = soup.select(".pagination a")
            if len(nav) > 0:
                next_page_url = nav[-1]['href']
                src_lang, tgt_lang, _ = self._get_lang(url)
                next_page = int(re.search("\?page=(\d+)", next_page_url)[1])
                if next_page < 9:
                    new_url = "%s/%s" % (url.split("?")[0], next_page_url)
                    self.add_job(new_url)
        except IndexError as e:
            print(e, nav)
        for url in new_urls:
            self.add_job(url)

        src_lang, tgt_lang, langs = self._get_lang(url)
        fn, dict_fn, phrase_fn = self.make_fn(src_lang, tgt_lang)

        src_word = soup.select("#phraseHeaderId span")
        if len(src_word) > 0:
            src_word = src_word[0].text.strip()
            tgt_words = '|'.join([p.text.strip() for p in soup.select(".text-info strong")])
            if tgt_words != "":
                self.add_result((dict_fn, "%s |||| %s\n" % (src_word, tgt_words)))

        phr_rows = soup.select('#translationExamples .tableRow')
        if len(phr_rows) > 0:
            for row in phr_rows:
                src_phr = row.select('div')[0].select('span span')[0].text.strip()
                tgt_phr = row.select('div')[1].select('span span')[0].text.strip()
                self.add_result((phrase_fn, "%s |||| %s\n" % (src_phr, tgt_phr)))

    @staticmethod
    def collect_results(context, result):
        file = context['files'][result[0]]
        file.write(result[1])
        context['unique_phrases'].add(result[1])

    @staticmethod
    def make_fn(src, tgt):
        fn = "%s_%s" % (src, tgt)
        return fn, fn + ".dict", fn + ".phr"

    @staticmethod
    def monitor(context, time_escape, last_stats):
        return {
            'unique phrases': len(context['unique_phrases']),
            'real time speed (phrases/sec)':
                round((len(context['unique_phrases']) - last_stats.get('unique_phrases', 0)) / time_escape, 2)
        }

    def _get_lang(self, url):
        url = self.clean_url(url)
        url_splits = [w for w in url.split("/") if w != ""]
        src_lang = url_splits[0]
        tgt_lang = url_splits[1].split('?')[0]
        return src_lang, tgt_lang, "%s_%s" % (src_lang, tgt_lang)

    def clean_url(self, url):
        """
        Clean the url here if you need. Url is the hash key deciding where one website was visited before.
        :param url:
        :return:
        """
        url = super(DictCrawler, self).clean_url(url)
        url = url.replace('&tmmode=MUST', "")
        return url


if __name__ == "__main__":
    with open('crawlers/glosbe/config.yml', 'r', encoding='utf-8') as f:
        config = yaml.load(f)
        print(json.dumps(config, indent=2))

    seed_lists = {}
    for k, v in config['seed_lists'].items():
        with open(v, 'r') as f:
            seed_lists[k] = [l.strip() for l in f.readlines()]

    for task in config['crawl_tasks']:
        seed_list = seed_lists.get(task['src'], None)
        DictCrawler.start(
            task_name=DictCrawler.make_fn(task['src'], task['tgt'])[0],
            proxy_pool="mixed",
            qps=config.get('qps', None),
            thread_num=config.get("max_threads", 3000),
            output_dir=config.get('output_dir', 'outputs'),
            restart=task.get('restart', False),
            src=task['src'],
            tgt=task['tgt'],
            seed_list=seed_list,
            seed_num=5000,
        )
