"""SimpleApp.py"""

import os
import re
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
    input = sc.wholeTextFiles(inputFile)
    return input.map(prepare)

def finalise(data, outputFile):
    """store data in given file"""
    data.saveAsTextFile(outputFile)


def transform(input, mapper, reducer):
    """map reduce framework"""
    return input.flatMap(mapper).groupByKey().map(reducer)


def convert_to_string((a, b)):
    return a + " " + str(b)

def mkstring(alist):
    """convert a list into a space-separated string"""
    return ' '.join([str(x) for x in alist])


def inverted_index(sc, inputFile, outputFile):
    # rdd = initialise(sc, inputFile, lambda line: (inputFile, line))
    dir = "file:" + os.path.dirname(__file__)+ "/" + inputFile + "/"
    rdd = sc.wholeTextFiles(inputFile)
    rdd = rdd.flatMap(lambda (k,v): [(x.lower(), k[len(dir):].replace(".txt", "")) for x in re.split(r'\s+|[.,!@#$%^&*\(\)\{\}_+=\\/<>:|\[\]\"\'?;]\s*', v)])
    rdd = rdd.groupByKey().mapValues(list).filter(lambda (k,v): k != "")
    rdd = rdd.map(lambda (k,v): (k,sorted(list(set(v)))))
    result = rdd.map(lambda (key, value): key + " # " + mkstring(value))
    finalise(result, outputFile)

# Take the input directory from command line

input_directory = raw_input("Please enter the directory of files or enter 'default' for the default test cases: ")
if input_directory.lower() == 'default':
    input_directory = "Ex5_Input_Files"

# Remove the output directory if it exists, so that the program doesn't crash when saving as text file at the "finalise" method
if os.path.isdir("Ex5_Inverted_Index.out"):
    shutil.rmtree("Ex5_Inverted_Index.out")


# Start the word count after pre-processing
inverted_index(sc, input_directory, "Ex5_Inverted_Index.out")
print "The output can found in Ex5_Inverted_Index.out directory."
