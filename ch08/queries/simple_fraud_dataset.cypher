//This file create a simple dataset for the tests. It was hard to find a real database so we created on
//Cut and paste this file in a neo4j browser

//Users
CREATE (Alessandro:User {name:"Alessandro"})
CREATE (Filippo:User {name:"Filippo"})
CREATE (Flavia:User {name:"Flavia"})
CREATE (Aurora:User {name:"Aurora"})
CREATE (John:User {name:"John"})
CREATE (Kyle:User {name:"Kyle"})

//Merchants
CREATE (Grocery:Merchant {name:"Grocery"})
CREATE (GasStation:Merchant {name:"Gas Station"})
CREATE (Supermarket:Merchant {name:"Supermarket"})
CREATE (JewelryStore:Merchant {name:"Jewelry Store"})
CREATE (ToyStore:Merchant {name:"Toy Store"})
CREATE (Amazon:Merchant {name:"Amazon"})
CREATE (ATM:Merchant {name:"ATM"})

//Transactions
CREATE (tx1:Transaction:Fraudulent {id: 1, amount: 2000.00, date:datetime()})
CREATE (tx1)-[:AT_MERCHANT]->(Grocery)
CREATE (Alessandro)-[:MAKES]->(tx1)

CREATE (tx2:Transaction {id: 2, amount: 35.00, date:datetime() - duration('P1D')})
CREATE (tx2)-[:AT_MERCHANT]->(GasStation)
CREATE (Alessandro)-[:MAKES]->(tx2)

CREATE (tx3:Transaction {id: 3, amount: 25.00, date:datetime() - duration('P2D')})
CREATE (tx3)-[:AT_MERCHANT]->(Supermarket)
CREATE (Alessandro)-[:MAKES]->(tx3)

CREATE (tx4:Transaction {id: 4, amount: 12.00, date:datetime() - duration('P3D')})
CREATE (tx4)-[:AT_MERCHANT]->(Amazon)
CREATE (Alessandro)-[:MAKES]->(tx4)

CREATE (tx1)<-[:HAS_NEXT]-(tx2)
CREATE (tx2)<-[:HAS_NEXT]-(tx3)
CREATE (tx3)<-[:HAS_NEXT]-(tx4)


CREATE (tx5:Transaction:Fraudulent {id: 5, amount: 1000.00, date:datetime()})
CREATE (tx5)-[:AT_MERCHANT]->(Supermarket)
CREATE (Aurora)-[:MAKES]->(tx5)

CREATE (tx6:Transaction {id: 6, amount: 35.00, date:datetime() - duration('P1D')})
CREATE (tx6)-[:AT_MERCHANT]->(Amazon)
CREATE (Aurora)-[:MAKES]->(tx6)

CREATE (tx7:Transaction {id: 7, amount: 25.00, date:datetime() - duration('P2D')})
CREATE (tx7)-[:AT_MERCHANT]->(GasStation)
CREATE (Aurora)-[:MAKES]->(tx7)

CREATE (tx8:Transaction {id: 8, amount: 200.00, date:datetime() - duration('P3D')})
CREATE (tx8)-[:AT_MERCHANT]->(JewelryStore)
CREATE (Aurora)-[:MAKES]->(tx8)

CREATE (tx5)<-[:HAS_NEXT]-(tx6)
CREATE (tx6)<-[:HAS_NEXT]-(tx7)
CREATE (tx7)<-[:HAS_NEXT]-(tx8)


CREATE (tx9:Transaction {id: 9, amount: 12.00, date:datetime()})
CREATE (tx9)-[:AT_MERCHANT]->(Grocery)
CREATE (Filippo)-[:MAKES]->(tx9)

CREATE (tx10:Transaction {id: 10, amount: 15.00, date:datetime() - duration('P1D')})
CREATE (tx10)-[:AT_MERCHANT]->(Amazon)
CREATE (Filippo)-[:MAKES]->(tx10)

CREATE (tx9)<-[:HAS_NEXT]-(tx10)


CREATE (tx11:Transaction:Fraudulent {id: 11, amount: 1000.00, date:datetime()})
CREATE (tx11)-[:AT_MERCHANT]->(JewelryStore)
CREATE (Flavia)-[:MAKES]->(tx11)

CREATE (tx12:Transaction {id: 12, amount: 30.00, date:datetime() - duration('P1D')})
CREATE (tx12)-[:AT_MERCHANT]->(ToyStore)
CREATE (Flavia)-[:MAKES]->(tx12)

CREATE (tx13:Transaction {id: 13, amount: 100.00, date:datetime() - duration('P2D')})
CREATE (tx13)-[:AT_MERCHANT]->(GasStation)
CREATE (Flavia)-[:MAKES]->(tx13)

CREATE (tx14:Transaction {id: 14, amount: 34.00, date:datetime() - duration('P3D')})
CREATE (tx14)-[:AT_MERCHANT]->(Amazon)
CREATE (Flavia)-[:MAKES]->(tx14)

CREATE (tx11)<-[:HAS_NEXT]-(tx12)
CREATE (tx12)<-[:HAS_NEXT]-(tx13)
CREATE (tx13)<-[:HAS_NEXT]-(tx14)


CREATE (tx15:Transaction:Fraudulent {id: 15, amount: 1000.00, date:datetime()})
CREATE (tx15)-[:AT_MERCHANT]->(ATM)
CREATE (John)-[:MAKES]->(tx15)

CREATE (tx16:Transaction {id: 16, amount: 30.00, date:datetime() - duration('P1D')})
CREATE (tx16)-[:AT_MERCHANT]->(Amazon)
CREATE (John)-[:MAKES]->(tx16)

CREATE (tx17:Transaction {id: 17, amount: 100.00, date:datetime() - duration('P2D')})
CREATE (tx17)-[:AT_MERCHANT]->(GasStation)
CREATE (John)-[:MAKES]->(tx17)

CREATE (tx15)<-[:HAS_NEXT]-(tx16)
CREATE (tx16)<-[:HAS_NEXT]-(tx17)


CREATE (tx18:Transaction {id: 18, amount: 12.00, date:datetime()})
CREATE (tx18)-[:AT_MERCHANT]->(Amazon)
CREATE (Kyle)-[:MAKES]->(tx18)

CREATE (tx19:Transaction {id: 19, amount: 15.00, date:datetime() - duration('P1D')})
CREATE (tx19)-[:AT_MERCHANT]->(Supermarket)
CREATE (Kyle)-[:MAKES]->(tx19)

CREATE (tx18)<-[:HAS_NEXT]-(tx19)





