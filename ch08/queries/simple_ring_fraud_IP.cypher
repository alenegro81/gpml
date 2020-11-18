CREATE CONSTRAINT ON (s:IPIstance) ASSERT (s.id) IS NODE KEY;


//run it all at once
MATCH (jimjam:User {accountId: "45322860293"})
MATCH (drwho:User {accountId: "45059804875"})
MATCH (robbob:User {accountId: "41098759500"})

CREATE (connection1:IPIstance {id:"166.184.50.48_2020-01-21", ip: "166.184.50.48", date: "2020-01-21"})
CREATE (connection2:IPIstance {id:"208.125.140.154_2020-01-19", ip: "208.125.140.154", date: "2020-01-19"})
CREATE (connection3:IPIstance {id:"74.248.71.164_2020-01-17", ip: "74.248.71.164", date: "2020-01-17"})

CREATE (drwho)-[:CONNECTED_FROM_ADDRESS]->(connection1)
CREATE (drwho)-[:CONNECTED_FROM_ADDRESS]->(connection3)
CREATE (robbob)-[:CONNECTED_FROM_ADDRESS]->(connection1)
CREATE (jimjam)-[:CONNECTED_FROM_ADDRESS]->(connection2)
CREATE (robbob)-[:CONNECTED_FROM_ADDRESS]->(connection2)


