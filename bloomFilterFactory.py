import sys
from pyspark import SparkContext
from RatingBloomFilter import RatingBloomFilter

if __name__ == "__main__":
    master = "local"
    if len(sys.argv) == 2:
        master = sys.argv[1]
    sc = SparkContext(master, "BloomFilterFactory")

    def get_movies(line):
        movie = line.split("\t")
        result = (0,"e")
        try:
            result = (round(float(movie[1])), movie[0])
        except:
            result = (11,"e")
        return result

    
# # Create Bloom Filters    
#     blooms = {}

#     def filter_creation(rating):
#         bloom = BloomFilter(max_elements=10000, error_rate=0.1)
#         blooms[rating] = bloom

#     def filter_filling(row):
#         entry = row.collect().split("\t")
#         try:
#             blooms[entry[1]].add(entry[0])
#         except KeyError:
#             print("Variable x is not defined")
#             filter_creation(row[1])
#             blooms[entry[1]].add(entry[0])
#         except:
#             print("Something else went wrong")

    blooms = {}

    seeds = [randint(0, 2147483647) for i in range(3)]

    def filter_creation(rating):
        bloom = RatingBloomFilter(20, seeds)
        blooms[rating] = bloom

    def create_tenx_bloom_filters(movie):
        try:
            blooms[movie(0)].add(movie(1))
        except KeyError:
            filter_creation(movie(0))
            blooms[movie(0)].add(movie(1))
            #maybe recursive
        except:
            print("Fuck them beaches, i like em wet not sandy")

    text = sc.textFile("data.tsv")

    #movies = sc.parallelize(text.take(10))

    movies = text.map(get_movies)
    movies.map(create_tenx_bloom_filters)#reduce
    #counts.saveAsTextFile("data/")
    for bloom in blooms:
        print(bloom)
    print("hello hi what are you doing i don't know")