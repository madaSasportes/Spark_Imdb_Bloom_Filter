from RatingBloomFilter import RatingBloomFilter
from random import randint

seeds = [randint(0, 2147483647) for i in range(3)]

rbf = RatingBloomFilter(20,seeds)
ids = [
    "Shrek 1: the classic",
    "Shrek 2: the best one",
    "Shrek 3: evrybody hates it, but i think it was fun",
    "Shrek 4: if Jack Black wrote a fanfic about him becoming a charachter in shrek"
    ]
rbf.fillUp(ids[0])
rbf.fillUp(ids[1])

# print(rbf.filter)

print(rbf.lookUp(ids[0]))
print(rbf.lookUp(ids[1]))
print(rbf.lookUp(ids[2]))
print(rbf.lookUp(ids[3]))
print(rbf.lookUp("Puss in boots: I mean it was a movie and it apparently gets a sequel"))


rbf1 = RatingBloomFilter(20,seeds)

rbf1.fillUp(ids[2])
rbf1.fillUp(ids[3])

print(rbf1.lookUp(ids[0]))
print(rbf1.lookUp(ids[1]))
print(rbf1.lookUp(ids[2]))
print(rbf1.lookUp(ids[3]))
print(rbf1.lookUp("Puss in boots: I mean it was a movie and it apparently gets a sequel"))

print(rbf.toBitString())
print(rbf1.toBitString())

rbf.merge_or(rbf1)

print(rbf.toBitString())

print(rbf.lookUp(ids[0]))
print(rbf.lookUp(ids[1]))
print(rbf.lookUp(ids[2]))
print(rbf.lookUp(ids[3]))
print(rbf.lookUp("Puss in boots: I mean it was a movie and it apparently gets a sequel"))

