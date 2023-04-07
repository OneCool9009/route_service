import json
from flask import Flask, request, Response
from flask_restful import Api, Resource
from werkzeug.serving import run_simple
from app import calc_clusters


class CalcClusters(Resource):
    @staticmethod
    def get():
        try:
            result_dct = calc_clusters(request.json)
            return Response(json.dumps(result_dct), 200)
        except Exception as err:
            return Response(json.dumps({'error': str(err)}), 500)


app = Flask(__name__)
api = Api(app)
api.add_resource(CalcClusters, '/')

if __name__ == '__main__':
    run_simple(hostname='localhost', port=5001, application=app, threaded=True)
