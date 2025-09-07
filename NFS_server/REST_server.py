from flask import Flask, jsonify, request
from flask_restful import Resource, Api


app = Flask(__name__)
api = Api(app)

class Hello(Resource):

    def get(self):
        return jsonify({
            'message' : 'hello world'
        })

    def post(self):
        data = request.get_json()

        return jsonify({
            'data' : data
        })

api.add_resource(Hello, '/')

# driver function
if __name__ == '__main__':
    app.run(debug = True)
