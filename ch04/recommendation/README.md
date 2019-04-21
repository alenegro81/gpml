CALL apoc.periodic.iterate(
"MATCH (u:User) WHERE NOT (u)-[:INTERESTED_IN]->() return u",
"MATCH (u)-[:RATED]->(m:Movie)-[:ACTS_IN|WRITES|DIRECTED|PRODUCES|HAS_GENRE]-(feature) WITH u, feature, count(feature) as occurrence WHERE occurrence > 2 MERGE (u)-[r:INTERESTED_IN]->(feature) ON CREATE SET r.weight = occurrence", {batchSize:100, parallel:false})


MATCH (:Movie)-[:ACTS_IN|WRITES|DIRECTED|PRODUCES|HAS_GENRE]-(feature)
set feature:Feature

MATCH (feature:Feature)
WITH distinct feature
ORDER BY id(feature)
MATCH (u:User {userId:"3198"})
OPTIONAL MATCH (u)-[r:INTERESTED_IN]->(feature)
with id(feature) as featureId, case r is null when true then 0 else r.weight end as value
return featureId, value;



CALL apoc.periodic.iterate(
"MATCH (m:Movie) with m",
"MATCH (feature:Feature) WITH distinct m,  feature ORDER BY id(feature) OPTIONAL MATCH (m)-[r:ACTS_IN|WRITES|DIRECTED|PRODUCES|HAS_GENRE]-(feature) with m, id(feature) as featureId, case r is null when true then 0 else 1 end as value order by m, featureId set m.vector = collect(value)", 
{batchSize:100, parallel:true})


CALL apoc.periodic.iterate(
"MATCH (m:Movie) return m limit 1",
"MATCH (movie:movie) WHERE id(movie) = id(m) MATCH (feature:Feature) WITH distinct movie, feature ORDER BY id(feature) OPTIONAL MATCH (m)-[r:ACTS_IN|WRITES|DIRECTED|PRODUCES|HAS_GENRE]-(feature) with m, id(feature) as featureId, case r is null when true then 0 else 1 end as value order by m, featureId WITH m, collect(value) as vector set m.vector = vector", 
{batchSize:100, parallel:false})
