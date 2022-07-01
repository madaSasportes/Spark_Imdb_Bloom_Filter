from RatingBloomFilter import RatingBloomFilter

rbf = RatingBloomFilter(20,3)
ids = [
    "Shrek 1: the classic",
    "Shrek 2: the best one",
    "Shrek 3: evrybody hates it, but i think it was fun",
    "Shrek 4: if Jack Black wrote a fanfic about him becoming a charachter in shrek"
    ]
rbf.fillUp(ids[0])
rbf.fillUp(ids[1])
rbf.fillUp(ids[2])
rbf.fillUp(ids[3])

# print(rbf.filter)

print(rbf.lookUp(ids[2]))
print(rbf.lookUp("Puss in boots: I mean it was a movie and it apparently gets a sequel"))
print(rbf.lookUp(ids[0]))