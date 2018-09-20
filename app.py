import os

from flask import Flask
from flask import request
from pyspark.sql import SparkSession


app = Flask(__name__)


def produce_hash(string):
    spark = SparkSession.builder.appName("PythonPi").getOrCreate()
    myhash = spark.sparkContext.parallelize(list(string)).map(lambda letter: (letter, 1)).reduceByKey(lambda x, y: x + y)
    ret = myhash.collect()
    spark.stop()
    return ret

@app.route("/")
def index():
    return "Python Flask SparkPi server running. Add the 'sparkpi' route to this URL to invoke the app."


@app.route("/lettercount")
def lettercount():
    mystr = str(request.args.get('string', "foobar"))
    my_hash = produce_hash(mystr)
    response = "Pi is roughly  and the letters repeated in the supplied string are {}".format(my_hash)

    return response


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
