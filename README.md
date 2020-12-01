# Synapse in a Day

Demos and labs for Synapse in a Day content.

- [Synapse in a Day](#synapse-in-a-day)
  - [Setup](#setup)
  - [Demos](#demos)
  - [Tailwind Traders - In search of a modern data warehouse solution](#tailwind-traders---in-search-of-a-modern-data-warehouse-solution)
  - [PowerPoint](#powerpoint)

## Setup

Follow the [demo setup instructions](https://github.com/ctesta-oneillmsft/asa-vtd) (external link).

## Demos

The [demos](./demos/) folder contains demo scripts for six modules, each lasting about 30 minutes apiece:

- [**Module 1: Realize Integrated Analytical Solutions with Azure Synapse Analytics**](demos/module1/README.md)
  - Surveying the Components of Azure Synapse Analytics
  - Exploring Azure Synapse Studio
  - Designing a Modern Data Warehouse using Azure Synapse Analytics
- [**Module 2: Optimize a Data Warehouse with Azure Synapse SQL Pools**](demos/module2/README.md)
  - Understanding developer features of Azure Synapse Analytics
  - Using data loading best practices in Azure Synapse Analytics
  - Optimizing data warehouse query performance in Azure Synapse Analytics
- [**Module 3: Perform Data Engineering with Azure Synapse Spark Pools**](demos/module3/README.md)
  - Ingesting data with Apache Spark notebooks in Azure Synapse Analytics
  - Transforming data with DataFrames in Spark Pools in Azure Synapse Analytics
  - Integrating SQL and Spark pools in Azure Synapse Analytics
- [**Module 4: Build automated data integration pipelines with Azure Synapse Pipelines**](demos/module4/README.md)
  - Petabyte-scale ingestion with Azure Synapse Pipelines
  - Code-free transformation at scale with Azure Synapse Pipelines
  - Orchestrate data movement and transformation in Azure Synapse Pipelines
- [**Module 5: Run interactive queries using Azure Synapse SQL Serverless**](demos/module5/README.md)
  - Querying a Data Lake Store using SQL Serverless  in Azure Synapse Analytics
  - Securing access to data through using SQL Serverless in Azure Synapse Analytics
- [**Module 6: Support Hybrid Transactional Analytical Processing with Azure Synapse Link**](demos/module6/README.md)
  - Configuring Azure Synapse Link with Cosmos DB
  - Querying Cosmos DB with Apache Spark for Synapse Analytics
  - Querying Cosmos DB with SQL Serverless for Synapse Analytics

## Tailwind Traders - In search of a modern data warehouse solution

![Tailwind Traders logo.](media/logo.png "Tailwind Traders")

Tailwind Traders has an online store where they sell a variety of products. These products are manufactured from a large number of suppliers that are located around the world. Tailwind Traders enables their suppliers to manage their product inventory directly through a product admin site.  The larger suppliers tend to have their own inventory management systems. They opt out of using the product admin site in favor of exporting their product data using an automated data feed.

Tailwind Traders has a team of data analysts. They analyze sales data and create reports that are intended to make the organization more effective at driving sales and procuring inventory. These reports go to a wide-variety of users, including suppliers, marketing, sales, and corporate leadership.  Their market analysis also includes considering new products offered by their network of suppliers, and anticipating consumer demand for those products. They share this information with invested parties to help plan new product development and production.

Tailwind Traders would like to gain business insights using historical, real-time, and predictive analytics.  The data may reside in both structured and unstructured data sources.  They want to enable their data engineers and data scientists to bring in and run complex queries over petabytes of structured data with billions of rows. At the same time, they want to enable data analysts to share a single source of truth and have a single workspace to collaborate and work with enriched data. They want to accomplish this while minimizing the number of tools and services required to ingest, transform, store, and query their data. They want their wide team that exists across various departments to use one shared tool. They hope they can increase collaboration, share best practices, centralize management, and mature their troubleshooting and monitoring process.

You have been hired as a consultant to work with the teams to help them achieve their aspirations of building a modern data warehouse. Based on their needs, you recommend Azure Synapse Analytics and lead members from each team toward Tailwind Trader’s goal and ultimate success.

## PowerPoint 

The [PowerPoint](./PowerPoint/) folder PowerPoint templates that contain supportin slides for each session. Should you wish to present this materials, you can use them as is, or make ammendments to meet your needs.
