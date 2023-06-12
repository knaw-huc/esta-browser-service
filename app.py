from flask import Flask, request, jsonify
import json
from elastic_index import Index
from mysql_handler import Db
import os


app = Flask(__name__)

es_config = {
    "url" : "localhost",
    "port" : "9200",
    "doc_type" : "manuscript",
    "ranges": ["year", "sub_voyage.slaves_total"]
}

db_config = {
    "host": os.environ.get("DB_HOST"),
    "database": os.environ.get("DB_DATABASE"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD")
}


index = Index(es_config)
db = Db(db_config)


@app.after_request
def after_request(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'POST, GET, OPTIONS'
    response.headers['Content-type'] = 'application/json'
    return response

@app.route("/")
def hello_world():
    retStruc = {"app": "ESTA service", "version": "0.9"}
    return json.dumps(retStruc)

@app.route("/facet", methods=['GET'])
def get_facet():
    facet = request.args.get("name")
    amount = request.args.get("amount")
    ret_struc = index.get_facet(facet + ".keyword", amount)
    return json.dumps(ret_struc)

@app.route("/filter-facet", methods=['GET'])
def get_filter_facet():
    facet = request.args.get("name")
    amount = request.args.get("amount")
    facet_filter = request.args.get("filter")
    ret_struc = index.get_filter_facet(facet + ".keyword", amount, facet_filter)
    return json.dumps(ret_struc)

@app.route("/browse", methods=['POST'])
def browse():
    struc = request.get_json()
    ret_struc = index.browse(struc["page"], struc["page_length"], struc["sortorder"] + ".keyword", struc["searchvalues"])
    return json.dumps(ret_struc)

@app.route("/voyage", methods=['GET'])
def voyage():
    id = request.args.get('id')
    voyage = db.get_voyage(id)
    return jsonify(voyage)

@app.route("/get_collection", methods=["POST"])
def get_collection():
    data = request.get_json()
    print(data)
    collection_items = index.get_collection_items(data["collection"])
    return json.dumps(collection_items);
@app.route("/get_global")
def get_global():
    voyages = db.get_global()
    return json.dumps(voyages)





#Start main program

if __name__ == '__main__':
    app.run()

