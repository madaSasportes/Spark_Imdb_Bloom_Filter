from random import randint
import mmh3

#need to add the Murmur Hush

class RatingBloomFilter:
    def __init__(self,filterSize,seeds):
        self.filter = [False for i in range(filterSize)]
        self.seeds = seeds

    def fillUp(self,key):
        size = len(self.filter)
        for seed in self.seeds:
            pos = abs(mmh3.hash(key,seed)) % size
            self.filter[pos] = True
    
    def lookUp(self,key):
        size = len(self.filter)
        result = True
        for seed in self.seeds:
            pos = abs(mmh3.hash(key,seed)) % size
            result = result and self.filter[pos]
        return result
    
    def merge_or(self,merge_filter):
        for i in range(len(self.filter)):
            self.filter[i] = self.filter[i] or merge_filter.filter[i]            

    def toBitString(self):
        result = ""
        for i in self.filter:
            result += "1" if i else "0"
        return result