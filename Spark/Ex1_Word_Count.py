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
    input = sc.textFile(inputFile)
    return input.map(prepare)

def finalise(data, outputFile):
    """store data in given file"""
    data.saveAsTextFile(outputFile)


def transform(input, mapper, reducer):
    """map reduce framework"""
    return input.flatMap(mapper).groupByKey().map(reducer)


def convert_to_string((a, b)):
    return a + " " + str(b)


# wordcount
# Update: Each word is converted to a lower case, and the final result to be written in the file is transformed to the desired output
# Example: ('hello', 2) will be written in the output file as: hello 2

def wordcount(sc, inputFile, outputFile):
    rdd = initialise(sc, inputFile, lambda line: ("NoKey", line))
    rdd = transform(rdd,
                       lambda (key, data): [(x.lower(), 1) for x in re.split(r'\s+|[.,!@#$%^&*\(\)\{\}_+=\\/<>:|\[\]\"\'?;]\s*', data)],
                       lambda (key, values): (key, sum(values)))

    result = rdd.filter(lambda (k, v): k != "").map(convert_to_string)
    finalise(result, outputFile)

# Take the input directory from command line
input_directory = raw_input("Please enter the directory of files or enter 'default' for the default test cases: ")
if input_directory.lower() == 'default':
    input_directory = "Ex1_Input_Files"


# Remove the output directory if it exists, so that the program doesn't crash when saving as text file at the "finalise" method
if os.path.isdir("Ex1_Word_Count.out"):
    shutil.rmtree("Ex1_Word_Count.out")

# Start the word count after pre-processing
wordcount(sc, input_directory, "Ex1_Word_Count.out")
print "The output can found in Ex1_Word_Count.out directory."
