# rest_server.py
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/echo", methods=["POST"])
def echo():
    data = request.get_json()
    return jsonify({"message": data["message"]})

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000)
