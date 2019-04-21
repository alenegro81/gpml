from sklearn.metrics.pairwise import cosine_similarity

pulp_fiction = [1,0,1,1,0,1,0]
the_punisher = [1,1,1,1,1,0,1]
kill_bill = [1,0,1,1,0,1,0]

print(cosine_similarity([pulp_fiction], [the_punisher]))
print(cosine_similarity([pulp_fiction], [kill_bill]))
print(cosine_similarity([kill_bill], [the_punisher]))