import random
from core.crawler import Crawler


class SimpleCrawler(Crawler):

    @property
    def base_url(self):
        return 'https://www.baidu.com'

    @staticmethod
    def prepare(context, args):
        """
        Do something preparation and return a list of start urls.
        Running in MAIN process, before all workers starting.
        :param context: store some variable shared in `collect_results`
        :param args: same as args passed to `start`
        :return: list of start urls
        """
        context['messages'] = []
        print("I am ready!")

        return ["/s?wd=hello_world"]

    @staticmethod
    def collect_results(context, result):
        """
        Handle the result. For example, save them to the file.
        You don't need to consider synchronization because it is running in ONE process.
        :param context: some variables saved in `collect_results`
        :param result: result added by `add_result` method
        """
        context['messages'].append(result)
        print(context['messages'])

    def parse(self, soup, url):
        """
        You should do 2 steps here:
        1. Parse the html, extract useful information and save them by calling `add_result`
        2. Extract NEXT urls and save them by calling `add_url`, making the crawler continue running.

        You should NOT write any thread-UNSAFE code here, such as writing to a file. Instead, you should
        pass the result to `collect_results` by calling `add_result`.
        :param soup: html parsed by bs4
        :param url: cleaned request url
        :return:
        """
        self.add_result("Scrape %s successfully" % url)
        for _ in range(1):
            new_url = "/s?wd=%d" % random.randint(0, 100000)
            self.add_job(new_url)


if __name__ == "__main__":
    # Start the crawler.
    SimpleCrawler.start(
        task_name="simple_crawler",
        proxy_pool="fake",
        thread_num=10,
        restart=True
    )
