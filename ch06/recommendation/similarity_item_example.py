from sklearn.metrics.pairwise import cosine_similarity

item3 = [0,1,0,0,1]
item7 = [1,0,1,0,1]
item9 = [0,1,1,1,1]
item12 = [1,1,1,1,1]
item23 = [1,1,0,0,0]
item65 = [0,1,0,1,0]
item85 = [1,0,0,1,0]
item248 = [0,1,1,1,0]
item346 = [1,0,1,1,1]
item562 = [1,0,0,0,0]

print("....")
print(cosine_similarity([item12], [item23]))
print(cosine_similarity([item12], [item65]))
print(cosine_similarity([item12], [item85]))
print(cosine_similarity([item12], [item248]))
print(cosine_similarity([item12], [item562]))

