import sys
from pyspark import SparkContext
from RatingBloomFilter import RatingBloomFilter

# initialize a new Spark Context to use for the execution of the script
sc = SparkContext(appName="IMDB-BLOOM-FILTER", master="yarn")

if __name__ == "__main__":
    master = "yarn"
    if len(sys.argv) == 2:
        master = sys.argv[1]

    def bloom_construction(movie):
        bloom= RatingBloomFilter(400000, [10,20,30])
        for i in range(len(movie[1])):
            #bloom.add(movie[1][i])
            bloom.fillUp(movie[1][i])
        return bloom

    def test_bloom(bloom):
        movies_in_groups = movies_in_groups_broad.value
        count = 0
        for i in range(len(movies_in_groups)):
            if movies_in_groups[i][0] != bloom [0]:
                for j in range(len(movies_in_groups[i][1])):
                   if bloom[1].lookUp(movies_in_groups[i][1][j]):
                        count += 1
        return count

    #Read-in Data and split it in strings
    data = sc.textFile("data.tsv")

    #Remove the header
    header = data.first()
    rmHeader = data.filter(lambda x : x != header)

    #Prepare Data
    rows = rmHeader.flatMap(lambda x: x.split("\n")) #Split Data in rows
    entries = rows.map(lambda x: (int(round(float(x.split("\t")[1]))), x.split("\t")[0])) #Create an RDD with ID and Rating per for each movie
    movies_in_groups_rdd = entries.groupByKey().map(lambda x : (x[0], list(x[1]))) #Group and Sort movies
    movies_in_groups_sorted = movies_in_groups_rdd.sortBy(lambda x: x[0])

    #Create Bloom Filters for every Rating
    blooms_rdd = movies_in_groups_sorted.map(lambda x:(x[0], bloom_construction(x)))

    #Membership-Testing
    movies_in_groups_broad = sc.broadcast(movies_in_groups_sorted.collect())
    total_movies_per_group = entries.countByKey()

    total_false_positives = blooms_rdd.map(lambda x: (x[0], test_bloom(x)))
    rate_false_positives = total_false_positives.map(lambda x: (x[0], x[1]/total_movies_per_group[x[0]]))

    #Collect Results
    fpt = total_false_positives.collect()
    fpr = rate_false_positives.collect()

    #Print Results
    for i in range(len(fpt)):
       print("Bloom Filter " +str(fpt[i][0]) + " gives back " + str(fpt[i][1]) + " false positives,"\
       +" that corresponds to a false posititvity rate of " + str(round(fpr[i][1]*100,3)) + " percent")