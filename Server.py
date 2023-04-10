import json
import os
import pandas as pd
import requests
from flask import Flask, request, Response
from flask_restful import Api, Resource
from werkzeug.serving import run_simple
from app import calc_clusters

PORTAL_URL = 'http://{путь_до_портала_гт2}/carriers-order/updateClusters'

app = Flask(__name__)
uploads_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data_storage')
if not os.path.exists(uploads_dir):
    os.mkdir(uploads_dir)


@app.route('/cluster/calculate', methods=['POST'])
def calc_cluster():
    try:
        data_file = request.files['file']
        data_file.save(os.path.join(uploads_dir, 'graph_data.xlsx'))

        df = pd.read_excel(os.path.join(uploads_dir, 'graph_data.xlsx'))
        if df.empty:
            return Response(json.dumps({'error': 'invalid file'}), 500)

        result_dct = calc_clusters()
        # requests.put(PORTAL_URL, data=json.dumps(result_dct))

        return Response(json.dumps(result_dct), 200)
    except Exception as err:
        return Response(json.dumps({'error': str(err)}), 500)


@app.route('/cluster/getCalculation', methods=['GET'])
def get():
    try:
        with open(os.path.dirname(os.path.realpath(__file__)) + '/result_storage/cluster_result.json') as json_file:
            result_dct = json.load(json_file)

        return Response(json.dumps(result_dct), 200)
    except Exception as err:
        return Response(json.dumps({'error': str(err)}), 500)


if __name__ == '__main__':
    run_simple(hostname='localhost', port=5001, application=app, threaded=True)
