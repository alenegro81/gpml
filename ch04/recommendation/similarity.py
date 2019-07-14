from sklearn.metrics.pairwise import cosine_similarity

pulp_fiction = [1,0,1,1,0,1,0]
the_punisher = [1,1,1,1,1,0,1]
kill_bill = [1,0,1,1,0,1,0]

print(cosine_similarity([pulp_fiction], [the_punisher]))
print(cosine_similarity([pulp_fiction], [kill_bill]))
print(cosine_similarity([kill_bill], [the_punisher]))

item1 = [3,4,3,1,5]
item4 = [3,3,5,2,4]
item5 = [3,5,4,1,0]
print("....")
print(cosine_similarity([item1], [item4]))
print(cosine_similarity([item4], [item5]))
print(cosine_similarity([item1], [item5]))