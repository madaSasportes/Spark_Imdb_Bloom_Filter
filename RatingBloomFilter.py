from random import randint
import mmh3

#need to add the Murmur Hush

class RatingBloomFilter:
    def __init__(self,filterSize,amountOfSeeds):
        self.filter = [False for i in range(filterSize)]
        #maybe with list(bytearray(filterSize))
        self.seeds = [randint(0, 2147483647) for i in range(amountOfSeeds)]

    def fillUp(self,rating):
        size = len(self.filter)
        for seed in self.seeds:
            pos = abs(mmh3.hash(rating,seed)) % size
            self.filter[pos] = True
    
    def lookUp(self,rating):
        size = len(self.filter)
        result = True
        for seed in self.seeds:
            pos = abs(mmh3.hash(rating,seed)) % size
            result = result and self.filter[pos]
        return result

