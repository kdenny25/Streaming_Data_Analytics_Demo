from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob
import re

def preprocessing(lines):
    words = lines.select(explode(split(lines.text, 't_end')).alias('text'))
    words = lines.na.replace('', None)
    words = words.na.drop()
    words = lines
    words = re.sub(r'http\S+', '', words)
    words = re.sub('@\w+', '', words)
    words = re.sub('#', '', words)
    words = re.sub('RT', '', words)
    words = re.sub(':', '', words)
    words = words.withColumn('text', F.regexp_replace('text', r'http\S+', ''))
    words = words.withColumn('text', F.regexp_replace('text', '@\w+', ''))
    words = words.withColumn('text', F.regexp_replace('text', '#', ''))
    words = words.withColumn('text', F.regexp_replace('text', 'RT', ''))
    words = words.withColumn('text', F.regexp_replace('text', ':', ''))
    return words

# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity

def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity

def noun_phrase_extraction(text):
    return TextBlob(text).noun_phrases

def get_phrase(words):
    noun_phrase_extraction_udf = udf(noun_phrase_extraction, StringType())
    words = words.withColumn('Noun Phrase', noun_phrase_extraction_udf('text'))
    return words
def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn('polarity', polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn('subjectivity', subjectivity_detection_udf('word'))
    return words