# Using flask to make an api 
# import necessary libraries and functions 
from flask import Flask, jsonify, request 

import os

from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *


# creating a Flask app 
app = Flask(__name__) 

# on the terminal type: curl http://127.0.0.1:5000/ 
# returns hello world when we use GET. 
# returns the data that we send when we use POST. 



class SparkData:

	def __init__(self):
		self._spark = SparkSession.builder\
    .master("local")\
    .appName("Data")\
    .getOrCreate()

		self._df1 = self._spark.read.csv("data1.csv", header=True, inferSchema=True)
		self._df2 = self._spark.read.csv("data2.csv", header=True, inferSchema=True)
		self._df3 = self._spark.read.csv("data3.csv", header=True, inferSchema=True)
		self._df4 = self._spark.read.csv("data4.csv", header=True, inferSchema=True)
		self._df5 = self._spark.read.csv("data5.csv", header=True, inferSchema=True)
		self._df6 = self._spark.read.csv("data5.csv", header=True, inferSchema=True)
		self._df = self._df1.union(self._df2).union(self._df3).union(self._df4).union(self._df5).union(self._df6)

	@property
	def getDataFrame(self):
		return self._df

	@property
	def getGoodStudentRegion(self):
		return self._df.filter(self._df.ects>=60).groupBy("ville").agg(count("ects").alias("count_ects")).sort(col("count_ects").desc()).toJSON().take(100)


def init():
	# os.system('cmd /c "curl -H "X-API-Key: 03207a10" https://my.api.mockaroo.com/data.csv > "files/data1.csv"')
	# os.system('cmd /c "curl -H "X-API-Key: 03207a10" https://my.api.mockaroo.com/data.csv > "files/data2.csv"')
	# os.system('cmd /c "curl -H "X-API-Key: 03207a10" https://my.api.mockaroo.com/data.csv > "files/data3.csv"')
	# os.system('cmd /c "curl -H "X-API-Key: 03207a10" https://my.api.mockaroo.com/data.csv > "files/data4.csv"')
	# os.system('cmd /c "curl -H "X-API-Key: 03207a10" https://my.api.mockaroo.com/data.csv > "files/data5.csv"')
	# os.system('cmd /c "curl -H "X-API-Key: 03207a10" https://my.api.mockaroo.com/data.csv > "files/data6.csv"')
	print("ok")

@app.route('/', methods = ['GET', 'POST']) 
def home(): 
	if(request.method == 'GET'): 

		return jsonify( sparkproj.getGoodStudentRegion ) 


# A simple function to calculate the square of a number 
# the number to be squared is sent in the URL when we use GET 
# on the terminal type: curl http://127.0.0.1:5000 / home / 10 
# this returns 100 (square of 10) 
@app.route('/home/<int:num>', methods = ['GET']) 
def disp(num): 

	return jsonify(sparkproj.getGoodStudentRegion) 


# driver function 
if __name__ == '__main__':
	init()

	sparkproj = SparkData()

	app.run(debug = True) 
