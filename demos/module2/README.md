# Optimize a Data Warehouse with Azure Synapse SQL Pools

In this demo, we show ways you can optimize data warehouse workloads that use the provisioned SQL pools. We cover useful developer features, how to define and use workload management and classification to control data loading, and methods to optimize query performance. The following table of contents describes and links to the elements of the demo:

- [Optimize a Data Warehouse with Azure Synapse SQL Pools](#optimize-a-data-warehouse-with-azure-synapse-sql-pools)
  - [Understanding developer features of Azure Synapse Analytics](#understanding-developer-features-of-azure-synapse-analytics)
    - [Using windowing functions](#using-windowing-functions)
    - [Approximate execution using HyperLogLog functions](#approximate-execution-using-hyperloglog-functions)
  - [Using data loading best practices in Azure Synapse Analytics](#using-data-loading-best-practices-in-azure-synapse-analytics)
    - [Implement workload management](#implement-workload-management)
    - [Create a workload classifier to add importance to certain queries](#create-a-workload-classifier-to-add-importance-to-certain-queries)
    - [Reserve resources for specific workloads through workload isolation](#reserve-resources-for-specific-workloads-through-workload-isolation)
  - [Optimizing data warehouse query performance in Azure Synapse Analytics](#optimizing-data-warehouse-query-performance-in-azure-synapse-analytics)
    - [Create hash distribution and columnstore index](#create-hash-distribution-and-columnstore-index)
    - [Improve table structure with partitioning](#improve-table-structure-with-partitioning)
    - [Use result set caching](#use-result-set-caching)

## Understanding developer features of Azure Synapse Analytics

### Using windowing functions

### Approximate execution using HyperLogLog functions

## Using data loading best practices in Azure Synapse Analytics

### Implement workload management

Specify workload importance

Use workload isolation

### Create a workload classifier to add importance to certain queries

Tailwind Traders has asked you if there is a way to mark queries executed by the CEO as more important than others, so they don't appear slow due to heavy data loading or other workloads in the queue. You decide to create a workload classifier and add importance to prioritize the CEO's queries.

### Reserve resources for specific workloads through workload isolation

## Optimizing data warehouse query performance in Azure Synapse Analytics

### Create hash distribution and columnstore index

### Improve table structure with partitioning

### Use result set caching
