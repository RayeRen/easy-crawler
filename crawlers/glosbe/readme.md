# Glosbe Crawler

## Prepare

To run glosbe crawler, you should check these items first:

1. Redis server has started and connectable;
2. Mixed proxy pool server has started and connectable;
3. All dependencies in `requirements.txt` have been installed, if not, run `pip install -r requirements.txt`;
4. `.env` file has been created and modified.  

## Run 

### Glosbe Dictionary Crawler 

1. Copy the `config.yml.example` to `config.yml`.
 
2. Read the comments in `crawlers/glosbe/config.yml` and modify the `crawlers/glosbe/config.yml`.

3. Run
 
```bash
## You must run this script in the root directory of REPO
# linux or mac
export PYTHONPATH=. && python crawlers/glosbe/dict_crawler.py
 
# windows
set PYTHONPATH=.
python python crawlers/glosbe/dict_crawler.py
```

### Run the Glosbe Statistics Crawler 

Run
 
```bash
## You must run this script in the root directory of REPO
# linux or mac
export PYTHONPATH=. && python crawlers/glosbe/stats_crawler.py
 
# windows
set PYTHONPATH=.
python crawlers/glosbe_stats_crawler.py
```

## Benchmark
- Single Machine
    - Glosbe dictionary crawler: 102.05 qps
    - Glosbe statistics crawler: 113.23 qps
