# Support Hybrid Transactional Analytical Processing with Azure Synapse Link

In this demo, we show how Azure Synapse Link enables you to seamlessly connect an Azure Cosmos DB account to your Synapse Analytics workspace. We walk through how to enable and configure Synapse link, then how to query the Azure Cosmos DB analytical store using Apache Spark and SQL Serverless. The following table of contents describes and links to the elements of the demo:

- [Support Hybrid Transactional Analytical Processing with Azure Synapse Link](#support-hybrid-transactional-analytical-processing-with-azure-synapse-link)
  - [Demo prerequisites](#demo-prerequisites)
  - [Configuring Azure Synapse Link with Cosmos DB](#configuring-azure-synapse-link-with-cosmos-db)
  - [Querying Azure Cosmos DB with Apache Spark for Synapse Analytics](#querying-azure-cosmos-db-with-apache-spark-for-synapse-analytics)
  - [Querying Azure Cosmos DB with SQL Serverless for Synapse Analytics](#querying-azure-cosmos-db-with-sql-serverless-for-synapse-analytics)

## Demo prerequisites

All demos use the same environment. If you have not done so already, Complete the [environment setup instructions](https://github.com/solliancenet/synapse-in-a-day-deployment) (external link).

## Configuring Azure Synapse Link with Cosmos DB

Tailwind Traders uses Azure Cosmos DB to store user profile data from their eCommerce site. The NoSQL document store provided by the Azure Cosmos DB SQL API provides the familiarity of managing their data using SQL syntax, while being able to read and write the files at a massive, global scale.

While Tailwind Traders is happy with the capabilities and performance of Azure Cosmos DB, they are concerned about the cost of executing a large volume of analytical queries over multiple partitions (cross-partition queries) from their data warehouse. They want to efficiently access all the data without needing to increase the Azure Cosmos DB request units (RUs). They have looked at options for extracting data from their containers to the data lake as it changes, through the Azure Cosmos DB change feed mechanism. The problem with this approach is the extra service and code dependencies and long-term maintenance of the solution. They could perform bulk exports from a Synapse Pipeline, but then they won't have the most up-to-date information at any given moment.

You decide to enable Synapse Link and enable the analytical store on their Azure Cosmos DB containers. With this configuration, all transactional data is automatically stored in a fully isolated column store. This store enables large-scale analytics against the operational data in Azure Cosmos DB, without impacting the transactional workloads or incurring resource unit (RU) costs.

## Querying Azure Cosmos DB with Apache Spark for Synapse Analytics

## Querying Azure Cosmos DB with SQL Serverless for Synapse Analytics
