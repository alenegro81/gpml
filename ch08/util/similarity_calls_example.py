from sklearn.metrics.pairwise import cosine_similarity

call_01 =       [1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0]
call_02 =       [0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0]
call_03 =       [1, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0]
call_04 =       [1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0]
call_05 =       [0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0]
call_06 =       [0, 1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0]
call_07_fraud = [0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0]
call_08_fraud = [0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0]
call_09_fraud = [0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0]
call_10 =       [0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1]
call_11_fraud = [0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0]
call_12_fraud = [0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0]

calls = {'call_01': call_01,
         'call_02': call_02,
         'call_03': call_03,
         'call_04': call_04,
         'call_05': call_05,
         'call_06': call_06,
         'call_07_fraud': call_07_fraud,
         'call_08_fraud': call_08_fraud,
         'call_09_fraud': call_09_fraud,
         'call_10': call_10,
         'call_11_fraud': call_11_fraud,
         'call_12_fraud': call_12_fraud}

print("....")
processed = []
for i in list(calls):

    for j in list(calls):
        if {'source': j, 'dest': i} not in processed and i != j:
            print("similarity between", i, j, cosine_similarity([calls[i]], [calls[j]]))
            processed += [{'source': j, 'dest': i}]
            processed += [{'source': i, 'dest': j}]

