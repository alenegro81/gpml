CALL apoc.export.csv.query('MATCH (t:Transaction)-[r:SIMILAR_TO]->(other:Transaction)
WHERE t.isFraud = 1
RETURN t.transactionId as transactionId, avg(r.weight) as weight, sum(r.weight) as totalSum', 'fraund.csv',
{batchSize:1000, delim: '\t', quotes: false, format: 'plain', header:true})


CALL apoc.export.csv.query('MATCH (t:Transaction)-[r:SIMILAR_TO]->(other:Transaction)
WHERE t.isFraud = 0
RETURN t.transactionId as transactionId, avg(r.weight) as weight, sum(r.weight) as totalSum', 'noFraund.csv',
{batchSize:1000, delim: '\t', quotes: false, format: 'plain', header:true})


CALL apoc.export.csv.query('MATCH (t:Transaction)-[r:SIMILAR_TO_COSINE]->(other:Transaction)
WHERE t.isFraud = 1
RETURN t.transactionId as transactionId, avg(r.weight) as weight, sum(r.weight) as totalSum', 'fraud_cosine.csv',
{batchSize:1000, delim: '\t', quotes: false, format: 'plain', header:true})


CALL apoc.export.csv.query('MATCH (t:Transaction)-[r:SIMILAR_TO_COSINE]->(other:Transaction)
WHERE t.isFraud = 0
RETURN t.transactionId as transactionId, avg(r.weight) as weight, sum(r.weight) as totalSum', 'noFraud_cosine.csv',
{batchSize:1000, delim: '\t', quotes: false, format: 'plain', header:true})


MATCH (t:Transaction)-[r:SIMILAR_TO]->(:Transaction)
WITH t.transactionId as transactionId, r.weight as weight, t.isFraud as fraud
ORDER BY transactionId, weight
WITH transactionId, fraud, collect(weight)[1] as weight
ORDER BY weight DESC
LIMIT 1000
WHERE fraud = 1
return count(distinct transactionId)

MATCH (t:Transaction)-[r:SIMILAR_TO]->(:Transaction)
WITH t.transactionId as transactionId, min(r.weight) as score, t.isFraud as fraud
order by score desc
LIMIT 1000
WHERE fraud = 1
return count(distinct transactionId)


MATCH (t:Transaction) with t LIMIT 10
MATCH (t)-[r:SIMILAR_TO_EXACT_2]->(ot)
WITH t, r.weight as weight, ot.transactionId as otherTransactionId
order by t.transactionId, r.weight
WITH t, collect(weight) as l2Weights, collect(otherTransactionId) as l2Trans
MATCH (t)-[r:SIMILAR_TO_EXACT]->(ot)
WITH t.transactionId as transactionId, l2Weights, l2Trans, r.weight as weight,  ot.transactionId as otherTransactionId
order by t.transactionId, r.weight
RETURN transactionId, l2Weights, l2Trans, collect(weight), collect(otherTransactionId)

//To evaluate the differencies
MATCH (t:Transaction) with t LIMIT 10
MATCH (t)-[r:SIMILAR_TO_EXACT_L2]->(ot)
WITH t, r.weight as weight, ot.transactionId as otherTransactionId
order by t.transactionId, r.weight
WITH t, collect(weight) as l2Weights, collect(otherTransactionId) as l2Trans
MATCH (t)-[r:SIMILAR_TO]->(ot)
WITH t.transactionId as transactionId, l2Weights, l2Trans, r.weight as weight,  ot.transactionId as otherTransactionId
order by t.transactionId, r.weight
RETURN transactionId, l2Weights, l2Trans, collect(weight), collect(otherTransactionId)

MATCH (t:Transaction) with t LIMIT 10000
MATCH (t)-[r:SIMILAR_TO]->(ot)
WITH t, r.weight as weight, ot.transactionId as otherTransactionId
order by t.transactionId, r.weight
WITH t, collect(weight) as l2Weights, collect(otherTransactionId) as l2Trans
MATCH (t)-[r:SIMILAR_TO_EXACT_L2]->(ot)
WITH t.transactionId as transactionId, l2Weights, l2Trans, r.weight as weight,  ot.transactionId as otherTransactionId
order by t.transactionId, r.weight
WITH transactionId, collect(otherTransactionId) = l2Trans as equals
WHERE equals = true
RETURN count(*)

ODIN
MATCH (t:Transaction)<-[:SIMILAR_TO]-(ot:Transaction)
WITH t.transactionId as tId, t.isFraud as fraud, count(distinct ot) as inDegree
ORDER BY inDegree ASC
LIMIT 1000
WHERE fraud = 1
RETURN count(*)

MATCH (t:Transaction)-[r:SIMILAR_TO_EXACT_TEST_mahalanobis]->(:Transaction)
WITH t.transactionId as transactionId, avg(r.weight) as score, t.isFraud as fraud
order by score desc
LIMIT 1000
WHERE fraud = 1
return count(distinct transactionId)

#140
MATCH (t:Transaction)-[r:SIMILAR_TO_EXACT_TEST_mahalanobis]->(:Transaction)
WITH t.transactionId as transactionId, r.weight as weight, t.isFraud as fraud
ORDER BY transactionId, weight
WITH transactionId, fraud, collect(weight)[23] as weight
ORDER BY weight DESC
LIMIT 1000
WHERE fraud = 1
return count(distinct transactionId)

#147
MATCH (t:Transaction)-[r:SIMILAR_TO_EXACT_100_MAHALANOBIS]->(:Transaction)
WITH t.transactionId as transactionId, sum(r.weight) as score, t.isFraud as fraud
order by score desc
LIMIT 1000
WHERE fraud = 1
return count(distinct transactionId)


CALL apoc.periodic.submit(
'louvain',
'CALL algo.louvain("Transaction", "SIMILAR_TO",
  {write:true, writeProperty:"louvainCommunityANN"})
YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis;'
);

MATCH (t:Transaction)
RETURN t.louvainCommunityANN, count(t) as occurrence, sum(t.isFraud) as fraudOccurrence, sum(t.isFraud)/count(t)*100 as percentage
ORDER BY fraudOccurrence desc

CALL apoc.periodic.submit(
'louvain',
'CALL algo.louvain("Transaction", "SIMILAR_TO_EXACT_400_MAHALANOBIS",
  {write:true, writeProperty:"louvainCommunityKNN_400_MAHALANOBIS"})
YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis;'
);

MATCH (t:Transaction)
RETURN t.louvainCommunityKNN_400_MAHALANOBIS, count(t) as occurrence, sum(t.isFraud) as fraudOccurrence, sum(t.isFraud)/count(t)*100 as percentage
ORDER BY fraudOccurrence desc

#310
MATCH (t:Transaction)-[r:SIMILAR_TO_EXACT_400_MAHALANOBIS]->(ot:Transaction)
WHERE t.louvainCommunityKNN_400_MAHALANOBIS = 13
WITH t.transactionId as transactionId, avg(r.weight) as score, t.isFraud as fraud
order by score desc
LIMIT 1000
WHERE fraud = 1
return count(distinct transactionId)


MATCH (t:Transaction)
WHERE t.isFraud = 0
WITH t, rand() as rand
ORDER BY rand
LIMIT 492
SET t:Test4


CALL apoc.periodic.submit(
'louvain',
'CALL algo.louvain("Test4", "SIMILAR_TO_EXACT_400_MAHALANOBIS_TEST4",
  {write:true, writeProperty:"louvainCommunityKNN_400_MAHALANOBIS_TEST4_BIS"})
YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis;'
);