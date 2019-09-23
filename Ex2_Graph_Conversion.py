"""SimpleApp.py"""

import os
import glob
import shutil
from pyspark import SparkContext

def quiet_logs( sc ):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

sc = SparkContext("local")

quiet_logs(sc)


## MapReduce Framework
def initialise(sc, inputFile, prepare):
    """Open a file and apply the prepare function to each line"""
    input = sc.textFile(inputFile)
    return input.map(prepare)


def finalise(data, outputFile):
    """store data in given file"""
    data.saveAsTextFile(outputFile)


# utility functions
def parse(ins):
    """converts "(a,b)" into ("a","b")"""
    s = ins.strip()
    result = ("", "")
    if len(s) > 0 and s[0] == '(':
        comma = s.find(',')
        if comma >= 0:
           end = s.find(')', comma)
           if end >= 0:
             result = (s[1: comma].strip(),
                       s[comma+1: end].strip())
    return result


def mkstring(alist):
    """convert a list into a space-separated string"""
    return ' '.join([str(x) for x in alist])


def bidirectional((k, v)):
    return [(k, v), (v, k)]


def adjacency_list(sc, inputFile, outputFile):
    rdd = initialise(sc, inputFile, parse)
    rdd = rdd.flatMap(bidirectional)
    rdd = rdd.groupByKey()
    rdd = rdd.map(lambda (key, value): (key, list(value)))
    result = rdd.map(lambda (key, value): key + " # " + mkstring(value))
    finalise(result, outputFile)

input_directory = raw_input("Please enter the directory of files or enter 'default' for the default test cases: ")
if input_directory.lower() == 'default':
    input_directory = "Graph_Input_Files"


# Remove the output directory if it exists, so that the program doesn't crash when saving as text file at the "finalise" method
if os.path.isdir("Ex2_Graph_Conversion.out"):
    shutil.rmtree("Ex2_Graph_Conversion.out")


adjacency_list(sc, input_directory, "Ex2_Graph_Conversion.out")
print "The output can found in Ex2_Graph_Conversion.out directory."
