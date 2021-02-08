//simple model, run it one by one
CREATE CONSTRAINT ON (s:Email) ASSERT (s.value) IS UNIQUE;
CREATE CONSTRAINT ON (s:PhoneNumber) ASSERT (s.value) IS UNIQUE;
CREATE CONSTRAINT ON (s:Address) ASSERT (s.value) IS UNIQUE;

//run it all at once
CREATE (alenegro:User {accountId: "49295987202", username: "alenegro", email: "mpd7xg@tim.it", name:"Hilda J Womack", phone_number: "580-548-1149", address: "4093 Cody Ridge Road - Enid, OK"})
CREATE (jimjam:User {accountId: "45322860293", username: "jimjam", email: "jam@mail.com", name:"Megan S Blubaugh", phone_number: "504-262-8173", address: "4093 Cody Ridge Road - Enid, OK"})
CREATE (drwho:User {accountId: "45059804875", username: "drwho", email: "mpd7xg@tim.it", name:"John V Danielson", phone_number: "504-262-8173", address: "4985 Rose Avenue - MOUNT HOPE, WI"})
CREATE (robbob:User {accountId: "41098759500", username: "robbob", email: "bob@google.com", name:"Robert C Antunez", phone_number: "352-588-92219", address: "2041 Bagwell Avenue - San Antonio, FL"})

CREATE (cc1:CreditCard {id: "793922"})
CREATE (ba1:BankAccount {id: "896857"})
CREATE (lo1:Loan {id: "885398"})
CREATE (alenegro)-[:OWNS]->(cc1)
CREATE (alenegro)-[:OWNS]->(ba1)
CREATE (alenegro)-[:OWNS]->(lo1)

CREATE (cc2:CreditCard {id: "482513"})
CREATE (ba2:BankAccount {id: "305693"})
CREATE (jimjam)-[:OWNS]->(cc2)
CREATE (jimjam)-[:OWNS]->(ba2)

CREATE (cc3:CreditCard {id: "631264"})
CREATE (ba3:BankAccount {id: "171215"})
CREATE (lo3:Loan {id: "432775"})
CREATE (drwho)-[:OWNS]->(cc3)
CREATE (drwho)-[:OWNS]->(ba3)
CREATE (drwho)-[:OWNS]->(lo3)

CREATE (ba4:BankAccount {id: "377703"})
CREATE (lo4:Loan {id: "859916"})
CREATE (robbob)-[:OWNS]->(ba4)
CREATE (robbob)-[:OWNS]->(lo4)

MERGE (alenegroEmail:Email {value: "mpd7xg@tim.it"})
MERGE (alenegroPhoneNumber:PhoneNumber {value: "580-548-1149"})
MERGE (alenegroAddress:Address {value: "4093 Cody Ridge Road - Enid, OK"})
CREATE (alenegro)-[:HAS_EMAIL]->(alenegroEmail)
CREATE (alenegro)-[:HAS_PHONE_NUMBER]->(alenegroPhoneNumber)
CREATE (alenegro)-[:HAS_ADDRESS]->(alenegroAddress)

MERGE (jimjamEmail:Email {value: "jam@mail.com"})
MERGE (jimjamPhoneNumber:PhoneNumber {value: "504-262-8173"})
MERGE (jimjamAddress:Address {value: "4093 Cody Ridge Road - Enid, OK"})
CREATE (jimjam)-[:HAS_EMAIL]->(jimjamEmail)
CREATE (jimjam)-[:HAS_PHONE_NUMBER]->(jimjamPhoneNumber)
CREATE (jimjam)-[:HAS_ADDRESS]->(jimjamAddress)

MERGE (drwhoEmail:Email {value: "mpd7xg@tim.it"})
MERGE (drwhoPhoneNumber:PhoneNumber {value: "504-262-8173"})
MERGE (drwhoAddress:Address {value: "4985 Rose Avenue - MOUNT HOPE, WI"})
CREATE (drwho)-[:HAS_EMAIL]->(drwhoEmail)
CREATE (drwho)-[:HAS_PHONE_NUMBER]->(drwhoPhoneNumber)
CREATE (drwho)-[:HAS_ADDRESS]->(drwhoAddress)

MERGE (robbobEmail:Email {value: "bob@google.com"})
MERGE (robbobPhoneNumber:PhoneNumber {value: "352-588-92219"})
MERGE (robbobAddress:Address {value: "2041 Bagwell Avenue - San Antonio, FL"})
CREATE (robbob)-[:HAS_EMAIL]->(robbobEmail)
CREATE (robbob)-[:HAS_PHONE_NUMBER]->(robbobPhoneNumber)
CREATE (robbob)-[:HAS_ADDRESS]->(robbobAddress)


