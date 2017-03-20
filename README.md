# Entities-Engine
I am an engine that manages all the entities in the system. I have two main parts:
1. Entities Supervisor (Single) -  create, merge and split families
2. Entity Manager (One per Entity) - handles family logic, subscribe to each son’s data source, select prefered son, update family state and produce a family entity.

This engine will utilize AKKA streams and AKKA stream Kafka. 
