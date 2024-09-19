-- testing elverclear for all cost and fees


-- Question 1: Does the subgraph have the USD value for the intents -> P


-- Question 2: Hyperlane fees - are they included in the protocol fees?
-- yes they are included in the protocol fees - and it avg for the no. of tx in the queue
-- Avg. the gas feefrom the intent queue and apply it to each intent -> to seperte it out

-- dedup in settlement intent and get the gas used * gas price + same for origin
-- get gas used in message table to get hyperlane fee(spoke -> hub + hub -> spoke)


-- Question 3: Are there two seperate types of fees? we apply:
-- 1. Fixed protocol Fee 2. Other BPS fees



-- Question 4:Messing fee vs Hyperlane fee- is it different?
-- gas cost to submit the tx in  origin_intents -> gas used missing