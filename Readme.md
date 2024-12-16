## Overview
The **Systems and Methods for Big and Unstructured Data (SMBUD)** project explores the creation and management of bibliographic databases using advanced technologies like Neo4J, MongoDB, and Spark. The project involves modeling data, importing datasets, and performing complex queries to extract meaningful insights from bibliographic information. This repository contains the documentation and queries implemented in three main phases, covered in the following documents:

1. **Chapter 1-2**: Details the first two deliveries, including the use of Neo4J and MongoDB for database modeling, data import, and querying.
2. **Chapter 3**: Focuses on the third delivery, utilizing Apache Spark for data manipulation and advanced queries.

## Structure of the Documents

### Chapter 1-2
This document provides:
- An **introduction** to the project goals and dataset characteristics, particularly focusing on bibliographic data.
- The **Entity-Relationship (E-R) Model**, describing entities such as Papers, Authors, Fields of Study (FOS), Venues, and Publishers, with their relationships modeled in Neo4J.
- **Data Import** processes into Neo4J, explaining dataset preprocessing and query scripts used for node and relationship creation, including examples of handling missing or incomplete data.
- A variety of **MongoDB Queries** showcasing operations like data creation, updates, filtering conditions, aggregations, and performance benchmarks.

### Chapter 3
This document expands the project by:
- Providing details on **data import using Spark**, including the creation of structured JSON files for Publications, Venues, Authors, Fields of Study, and their relationships.
- Highlighting the use of **Spark-based queries**, which include:
  - Adding and updating data fields dynamically.
  - Filtering data through complex conditions using WHERE, JOIN, GROUP BY, HAVING, and nested queries.
  - Performing **aggregations** and **optimizing query performance** for large datasets.

## Key Features
- **Database Modeling**: Comprehensive E-R modeling using Neo4J and MongoDB to structure bibliographic data effectively.
- **Efficient Queries**: Implementation of optimized queries in Cypher (Neo4J), MongoDB, and Spark to handle diverse data requirements.
- **Performance Analysis**: Detailed evaluation of query execution times, highlighting bottlenecks and optimization strategies.
- **Data Processing with Spark**: Advanced data transformations and distributed query processing for scalability.

## Authors
- Stefano Baroni
- Luca Brembilla
- Alessia Menozzi
- Caterina Giardi
- Rocco Agnello

---