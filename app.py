import os

from flask import Flask
from flask import request
from pyspark.sql import SparkSession


app = Flask(__name__)


def produce_pi(scale):
    spark = SparkSession.builder.appName("PythonPi").getOrCreate()
    n = 100000 * scale

    def f(_):
        from random import random
        x = random()
        y = random()
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = spark.sparkContext.parallelize(
        range(1, n + 1), scale).map(f).reduce(lambda x, y: x + y)
    spark.stop()
    pi = 4.0 * count / n
    return pi

def produce_hash(string):
    spark = SparkSession.builder.appName("PythonPi").getOrCreate()
    hash = spark.sparkContext.parallelize(list(string)).map(lambda letter: (letter, 1)).reduceByKey(lambda x, y: x + y)
    return hash

@app.route("/")
def index():
    return "Python Flask SparkPi server running. Add the 'sparkpi' route to this URL to invoke the app."


@app.route("/sparkpi")
def sparkpi():
    scale = int(request.args.get('scale', 2))
    mystr = str(request.args.get('string', "foobar"))
    pi = produce_pi(scale)
    my_hash = produce_hash(mystr)
    response = "Pi is roughly {} and the letters repeated in the supplied string are {}".format(pi, my_hash)

    return response


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
