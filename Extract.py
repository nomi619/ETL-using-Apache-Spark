import urllib.request
import gzip
import pandas as pd

urllib.request.urlretrieve("https://datasets.imdbws.com/title.ratings.tsv.gz", "title.ratings.tsv.gz")
urllib.request.urlretrieve("https://datasets.imdbws.com/title.basics.tsv.gz", "title.basics.tsv.gz")
urllib.request.urlretrieve("https://datasets.imdbws.com/name.basics.tsv.gz", "name.basics.tsv.gz")