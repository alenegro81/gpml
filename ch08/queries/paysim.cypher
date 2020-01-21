CALL apoc.periodic.submit(
'louvain',
'CALL algo.louvain("Entity", "TRANSFER_TO",
  {write:true, writeProperty:"louvainCommunity"})
YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis;'
);


