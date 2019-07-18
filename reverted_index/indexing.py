import sys
import os
import re
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, concat, col, lit


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


def text_process(sc, input_folder, file, rdd_list):
    input_ = sc.textFile(input_folder+file)
    words = input_.flatMap(normalizeWords)
    word_map = words\
        .map(lambda x: (x, file))\
        .reduceByKey(lambda x, y: x)
# word, word
    rdd_list.append(word_map)


def main(input_folder, output_folder):
    conf = SparkConf().setMaster("local").setAppName("WordCount")
    sc = SparkContext(conf=conf)
    files = os.listdir(input_folder)
    print('#'*50)
    rdd_list = []
    for file in files:
        print('#'*50)
        print("processing " + file)
        if file != '.ds_store':
            text_process(sc, input_folder, file, rdd_list)
    print('#'*50)

    rdds = sc.union(rdd_list)\
        .map(lambda x: (x[0], [int(x[1])]))\
        .reduceByKey(lambda x, y: x+y)

    word_dic = rdds\
        .map(lambda x: x[0])\
        .zipWithIndex()

    # word_dic.saveAsTextFile(output_folder + 'dictionary')

    word_dic = word_dic.collectAsMap()
    rdds = rdds\
        .map(lambda x: (word_dic[x[0]], x[1]))

    rdds.saveAsTextFile(output_folder + 'reverse_index')


if __name__ == '__main__':
    from sys import argv

    input_folder = './data/indexing/'
    output_folder = './output/'
    main(input_folder, output_folder)
