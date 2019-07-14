from sklearn.metrics.pairwise import cosine_similarity

session1 = [0,1,0,1,1,0,1,0,1,1]
session2 = [1,0,1,1,1,1,0,1,0,0]
session3 = [0,1,1,1,0,0,0,1,1,0]
session4 = [0,0,1,1,0,1,1,1,1,0]
session5 = [1,1,1,1,0,0,0,0,1,0]

print("....")
print(cosine_similarity([session5], [session1])) #0.54772256
print(cosine_similarity([session5], [session2])) #0.54772256
print(cosine_similarity([session5], [session3])) #0.8
print(cosine_similarity([session5], [session4])) #0.54772256

