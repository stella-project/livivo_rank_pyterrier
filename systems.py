import os
import jsonlines
import pandas as pd
import pyterrier as pt
if not pt.started():
  pt.init()


def _livivo_doc_iter():
    for file in os.listdir("./data/livivo/documents/"):
        if file.endswith(".jsonl"):
            with jsonlines.open(os.path.join("./data/livivo/documents", file)) as reader:
                for obj in reader:
                    title = obj.get('TITLE') or ''
                    title = title[0] if type(title) is list else title
                    abstract = obj.get('ABSTRACT') or ''
                    abstract = abstract[0] if type(abstract) is list else abstract
                    yield {'docno': obj.get('DBRECORDID'), 'text': ' '.join([title, abstract])}


class Ranker(object):

    def __init__(self):
        self.idx = None

    def index(self):
        iter_indexer = pt.IterDictIndexer("./index")
        doc_iter = _livivo_doc_iter()
        indexref = iter_indexer.index(doc_iter)
        self.idx = pt.IndexFactory.of(indexref)

    def rank_publications(self, query, page, rpp):

        itemlist = []

        if query is not None:
            if self.idx is None:
                try:
                    self.idx = pt.IndexFactory.of('./index/data.properties')
                except Exception as e:
                    print('No index available: ', e)

            if self.idx is not None:
                topics = pd.DataFrame.from_dict({'qid': [0], 'query': [query]})
                retr = pt.BatchRetrieve(self.idx, controls={"wmodel": "TF_IDF"})
                retr.setControl("wmodel", "TF_IDF")
                retr.setControls({"wmodel": "TF_IDF"})
                res = retr.transform(topics)
                itemlist = list(res['docno'][page * rpp:(page + 1) * rpp])

        return {
            'page': page,
            'rpp': rpp,
            'query': query,
            'itemlist': itemlist,
            'num_found': len(itemlist)
        }


class Recommender(object):

    def __init__(self):
        self.idx = None

    def index(self):
        pass

    def recommend_datasets(self, item_id, page, rpp):

        itemlist = []

        return {
            'page': page,
            'rpp': rpp,
            'item_id': item_id,
            'itemlist': itemlist,
            'num_found': len(itemlist)
        }

    def recommend_publications(self, item_id, page, rpp):

        itemlist = []

        return {
            'page': page,
            'rpp': rpp,
            'item_id': item_id,
            'itemlist': itemlist,
            'num_found': len(itemlist)
        }
