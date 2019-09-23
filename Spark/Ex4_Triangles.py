"""SimpleApp.py"""
import os
import shutil
import itertools
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


def combination((k, l)):
    return k, list(itertools.combinations(sorted(l), 2))


def bidirectional((k, v)):
    return [(k, v), (v, k)]


def convert_to_string((a, b)):
    return a + " " + str(b)


def triangles(sc, inputFile, outputFile):
    rdd = initialise(sc, inputFile, parse)
    rdd = rdd.flatMap(bidirectional)
    rdd = rdd.groupByKey()
    rdd = rdd.map(lambda (key, value): (key, list(value)))

    rdd = rdd.map(combination)

    rdd = rdd.flatMap(lambda (k, v): [(tuple(sorted((key, value, k))),1) for (key, value) in v])
    rdd = rdd.groupByKey()
    rdd = rdd.map(lambda (key, values): (key, sum(values)))

    rdd = rdd.filter(lambda (key,value): value == 3)

    rdd = rdd.flatMap(lambda(key, value):[(i,1) for i in key]).groupByKey()
    result = rdd.map(lambda (key, values): (key, sum(values))).map(convert_to_string)
    finalise(result, outputFile)

input_directory = raw_input("Please enter the directory of files or enter 'default' for the default test cases: ")
if input_directory.lower() == 'default':
    input_directory = "Graph_Input_Files"

# Remove the output directory if it exists, so that the program doesn't crash when saving as text file at the "finalise" method
if os.path.isdir("Ex4_Triangles.out"):
    shutil.rmtree("Ex4_Triangles.out")


triangles(sc, input_directory, "Ex4_Triangles.out")
print "The output can found in Ex4_Triangles.out directory."