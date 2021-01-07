# Recommend Movies using Content-Based Approach
As discussed in the book, in order to make the recommendation faster we need to adjust a bit the graph model after the import.
This README.md contains the instruction to perform this adjustment.  

## How to install apoc 
The database is not big, but could happen that you have a big database to adjust. 
To manage operations that affects a lot of nodes it is the case to leverage the "iterate" procedure available in APOC (a neo4j library of procedures: https://neo4j.com/developer/neo4j-apoc/).
It allows to run the cypher queries in batches avoiding to have a big commit at the end.

Download the library from here: https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases.
Select the version that suits your Neo4j version and copy it in the plugins directory in your neo4j installation. 

In the neo4j config file add the following line:
```
dbms.security.procedures.allowlist=apoc.*,gds.*
``` 

Restart neo4j. 

## Graph model refinement 
The following queries (as explained in the book) re-adjust the model created during the import to achieve the goals of recommendation easier.

### Add the INTERESTED_IN relationship

```
CALL apoc.periodic.iterate(
"MATCH (u:User) WHERE NOT EXISTS((u)-[:INTERESTED_IN]->()) return u",
"MATCH (u)-[:RATES]->(m:Movie)-[:ACTS_IN|WRITES|DIRECTED|PRODUCES|HAS_GENRE]-(feature) 
WITH u, feature, count(feature) as occurrence 
WHERE occurrence > 2 
MERGE (u)-[r:INTERESTED_IN]->(feature) 
ON CREATE SET r.weight = occurrence", 
{batchSize:100, parallel:false})
```

```
MATCH (:Movie)-[:ACTS_IN|WRITES|DIRECTED|PRODUCES|HAS_GENRE]-(feature)
set feature:Feature
```

```
MATCH (feature:Feature)
WITH distinct feature
ORDER BY id(feature)
MATCH (u:User {userId:"3198"})
OPTIONAL MATCH (u)-[r:INTERESTED_IN]->(feature)
with id(feature) as featureId, case r is null when true then 0 else r.weight end as value
return featureId, value;
```



```
CALL apoc.periodic.iterate(
"MATCH (m:Movie) with m",
"MATCH (feature:Feature) 
WITH distinct m,  feature 
ORDER BY id(feature) 
OPTIONAL 
MATCH (m)-[r:ACTS_IN|WRITES|DIRECTED|PRODUCES|HAS_GENRE]-(feature) 
WITH m, id(feature) as featureId, case r is null when true then 0 else 1 end as value order by m, featureId 
set m.vector = collect(value)", 
{batchSize:100, parallel:true})
```


```
CALL apoc.periodic.iterate(
"MATCH (m:Movie) return m",
"MATCH (movie:movie) 
WHERE id(movie) = id(m) 
MATCH (feature:Feature) 
WITH distinct movie, feature 
ORDER BY id(feature) 
OPTIONAL MATCH (m)-[r:ACTS_IN|WRITES|DIRECTED|PRODUCES|HAS_GENRE]-(feature) 
WITH m, id(feature) as featureId, case r is null when true then 0 else 1 end as value order by m, featureId 
WITH m, collect(value) as vector 
SET m.vector = vector", 
{batchSize:100, parallel:false})
```

