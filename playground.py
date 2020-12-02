import os
import jsonlines

def _livivo_doc_iter():
    for file in os.listdir("./data/livivo/documents/"):
        if file.endswith(".jsonl"):
            with jsonlines.open(os.path.join("./data/livivo/documents", file)) as reader:
                for obj in reader:
                    title = obj.get('TITLE') or ''
                    title = title[0] if type(title) is list else title
                    abstract = obj.get('ABSTRACT') or ''
                    abstract = abstract[0] if type(abstract) is list else abstract
                    print({'docno': obj.get('DBRECORDID'), 'text': ' '.join([title, abstract])})


# _livivo_doc_iter()

os.environ['JAVA_HOME'] = "/usr/lib/jvm/java-11-openjdk-amd64/"

import pyterrier as pt
if not pt.started():
  pt.init()

import pandas as pd

query = "biology"
page = 0
rpp = 10

idx = pt.IndexFactory.of('./index/data.properties')
topics = pd.DataFrame.from_dict({'qid': [0], 'query': [query]})
retr = pt.BatchRetrieve(idx, controls={"wmodel": "TF_IDF"})
retr.setControl("wmodel", "TF_IDF")
retr.setControls({"wmodel": "TF_IDF"})
res = retr.transform(topics)
itemlist = list(res['docno'][page * rpp:(page + 1) * rpp])
print(itemlist)