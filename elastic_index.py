import json

from elasticsearch import Elasticsearch
import math


class Index:
    def __init__(self, config):
        self.config = config
        # self.es = Elasticsearch(hosts=[{"host": "ecodices_es"}], retry_on_timeout=True)
        self.client = Elasticsearch()

    def no_case(self, str_in):
        str = str_in.strip()
        ret_str = ""
        if (str != ""):
            for char in str:
                ret_str = ret_str + "[" + char.upper() + char.lower() + "]"
        return ret_str + ".*"

    def get_facet(self, field, amount):
        ret_array = []
        response = self.client.search(
            index="esta",
            body=
            {
                "size": 0,
                "aggs": {
                    "names": {
                        "terms": {
                            "field": field,
                            "size": amount,
                            "order": {
                                "_term": "asc"
                            }
                        }
                    }
                }
            }
        )
        for hits in response["aggregations"]["names"]["buckets"]:
            buffer = {"key": hits["key"], "doc_count": hits["doc_count"]}
            ret_array.append(buffer)
        return ret_array

    def get_filter_facet(self, field, amount, facet_filter):
        ret_array = []
        response = self.client.search(
            index="esta",
            body={
            "size": 0,
            "aggs": {
                "nested_terms": {
                    "nested": {
                        "path": "sub_voyage"
                    },
                    "aggs": {
                        "filter": {
                            "filter": {
                                "regexp": {
                                    field: facet_filter + ".*"
                                }
                            },
                            "aggs": {
                                "names": {
                                    "terms": {
                                        "field": field + ".keyword",
                                        "size": amount
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        )

        for hits in response["aggregations"]["nested_terms"]["filter"]["names"]["buckets"]:
            buffer = {"key": hits["key"], "doc_count": hits["doc_count"]}
            ret_array.append(buffer)
        return ret_array


    def browse(self, page, length, orderFieldName, searchvalues):
        int_page = int(page)
        start = (int_page - 1) * length
        matches = []

        if searchvalues == []:
            response = self.client.search(
                index="esta",
                body={"query": {
                    "match_all": {}},
                    "_source": ["year", "summary", "voyage_id", "sub_voyage.sub_dept_location", "sub_voyage.dep_latitude", "sub_voyage.dep_logitude", "sub_voyage.arrival_latitude", "sub_voyage.arrival_longitude", "sub_voyage.sub_arrival_location"],
                    "size": length,
                    "from": start
                }
            )
        else:
            for item in searchvalues:
                if item["field"] == "FREE_TEXT":
                    for value in item["values"]:
                        matches.append({"multi_match": {"query": value, "fields": ["*"]}})
                else:
                    if item["field"] in self.config["ranges"]:
                        range_values = item["values"][0]
                        r_array = range_values.split('-')
                        matches.append({"range": {item["field"]: {"gte": r_array[0], "lte": r_array[1]}}})
                    else:
                        if len(item["values"]) > 1:
                            matches.append({"terms": {item["field"] + ".keyword": item["values"]}})
                        else:
                            matches.append({"term": {item["field"] + ".keyword": item["values"][0]}})

            response = self.client.search(
                index="esta",
                body={"query": {
                    "bool": {
                        "filter": matches
                    }},
                    "_source": ["year", "summary", "voyage_id", "sub_voyage.sub_dept_location", "sub_voyage.dep_latitude", "sub_voyage.dep_logitude", "sub_voyage.arrival_latitude", "sub_voyage.arrival_longitude", "sub_voyage.sub_arrival_location"],
                    "size": length,
                    "from": start
                }
            )


        ret_array = {"amount": response["hits"]["total"]["value"],
                     "pages": math.ceil(response["hits"]["total"]["value"] / length), "items": []}
        for item in response["hits"]["hits"]:
            tmp_arr = item["_source"]
            tmp_arr["_id"] = item["_id"]
            ret_array["items"].append(tmp_arr)
        return ret_array
