from random import randint

#need to add the Murmur Hush

class RatingBloomFilter:
    def __init__(self,filterSize,amountOfSeeds):
        self.filter = False * filterSize
        #maybe with list(bytearray(filterSize))
        self.seeds = [randint() for i in range(amountOfSeeds)]

    def fillUp(self,rating):
        size = len(self.filter)
        for seed in self.seeds:
            pos = abs(hash(rating)) % size
            self.filter[pos] = True

