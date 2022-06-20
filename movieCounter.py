import sys
from pyspark import SparkContext

if __name__ == "__main__":
    master = "local"
    if len(sys.argv) == 2:
        master = sys.argv[1]
    sc = SparkContext(master, "WordCount")

    def get_rating(line):
        movie = line.split("\t")
        result = "e"
        try:
            result = str(round(float(movie[1])))
        except:
            result = "t"
        return result

    text = sc.textFile("data.tsv")

    #movies = sc.parallelize(text.take(10))

    ratings = text.map(get_rating)
    ones = ratings.map(lambda b: ( b, 1 ))
    counts = ones.reduceByKey(lambda x, y: x + y)
    #counts.saveAsTextFile("data/")
    sum = 0
    for i in counts.collect():
        sum = sum + i[1]
        print(i)
    print(sum)