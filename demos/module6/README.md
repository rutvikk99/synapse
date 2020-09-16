# Support Hybrid Transactional Analytical Processing with Azure Synapse Link

In this demo, we show how Azure Synapse Link enables you to seamlessly connect an Azure Cosmos DB account to your Synapse Analytics workspace. We walk through how to enable and configure Synapse link, then how to query the Azure Cosmos DB analytical store using Apache Spark and SQL Serverless. The following table of contents describes and links to the elements of the demo:

- [Support Hybrid Transactional Analytical Processing with Azure Synapse Link](#support-hybrid-transactional-analytical-processing-with-azure-synapse-link)
  - [Demo prerequisites](#demo-prerequisites)
  - [Configuring Azure Synapse Link with Cosmos DB](#configuring-azure-synapse-link-with-cosmos-db)
    - [Enable Azure Synapse Link](#enable-azure-synapse-link)
    - [Create a new Azure Cosmos DB container](#create-a-new-azure-cosmos-db-container)
    - [Create and run a copy pipeline](#create-and-run-a-copy-pipeline)
  - [Querying Azure Cosmos DB with Apache Spark for Synapse Analytics](#querying-azure-cosmos-db-with-apache-spark-for-synapse-analytics)
  - [Querying Azure Cosmos DB with SQL Serverless for Synapse Analytics](#querying-azure-cosmos-db-with-sql-serverless-for-synapse-analytics)

## Demo prerequisites

- All demos use the same environment. If you have not done so already, Complete the [environment setup instructions](https://github.com/solliancenet/synapse-in-a-day-deployment) (external link).
- You need to have completed [Module 4](../module4/README.md).
  - You have created the Azure Cosmos DB linked service.
  - You have created the `asal400_customerprofile_cosmosdb` integration data set.

## Configuring Azure Synapse Link with Cosmos DB

Tailwind Traders uses Azure Cosmos DB to store user profile data from their eCommerce site. The NoSQL document store provided by the Azure Cosmos DB SQL API provides the familiarity of managing their data using SQL syntax, while being able to read and write the files at a massive, global scale.

While Tailwind Traders is happy with the capabilities and performance of Azure Cosmos DB, they are concerned about the cost of executing a large volume of analytical queries over multiple partitions (cross-partition queries) from their data warehouse. They want to efficiently access all the data without needing to increase the Azure Cosmos DB request units (RUs). They have looked at options for extracting data from their containers to the data lake as it changes, through the Azure Cosmos DB change feed mechanism. The problem with this approach is the extra service and code dependencies and long-term maintenance of the solution. They could perform bulk exports from a Synapse Pipeline, but then they won't have the most up-to-date information at any given moment.

You decide to enable Azure Synapse Link for Cosmos DB and enable the analytical store on their Azure Cosmos DB containers. With this configuration, all transactional data is automatically stored in a fully isolated column store. This store enables large-scale analytics against the operational data in Azure Cosmos DB, without impacting the transactional workloads or incurring resource unit (RU) costs. Azure Synapse Link for Cosmos DB creates a tight integration between Azure Cosmos DB and Azure Synapse Analytics, which enables Tailwind Traders to run near real-time analytics over their operational data with no-ETL and full performance isolation from their transactional workloads.

By combining the distributed scale of Cosmos DB's transactional processing with the built-in analytical store and the computing power of Azure Synapse Analytics, Azure Synapse Link enables a Hybrid Transactional/Analytical Processing (HTAP) architecture for optimizing Tailwind Trader's business processes. This integration eliminates ETL processes, enabling business analysts, data engineers & data scientists to self-serve and run near real-time BI, analytics, and Machine Learning pipelines over operational data.

### Enable Azure Synapse Link

1. Navigate to the Azure portal (<https://portal.azure.com>) and open the `synapse-in-a-day` resource group (or whichever resource group you are using for the demo).

2. Select the **Azure Cosmos DB account**.

    ![The Azure Cosmos DB account is highlighted.](media/resource-group-cosmos.png "Azure Cosmos DB account")

3. Select **Features** in the left-hand menu **(1)**, then select **Azure Synapse Link (2)**.

    ![The Features blade is displayed.](media/cosmos-db-features.png "Features")

4. Select **Enable**.

    ![Enable is highlighted.](media/synapse-link-enable.png "Azure Synapse Link")

    Before we can create an Azure Cosmos DB container with an analytical store, we must first enable Azure Synapse Link.

### Create a new Azure Cosmos DB container

Tailwind Traders has an Azure Cosmos DB container named `OnlineUserProfile01`. Since we enabled the Azure Synapse Link feature _after_ the container was already created, we cannot enable the analytical store on the container. We will create a new container that has the same partition key and enable the analytical store.

After creating the container, we will create a new Synapse Pipeline to copy data from the `OnlineUserProfile01` container to the new one.

1. Select **Data Explorer** on the left-hand menu.

    ![The menu item is selected.](media/data-explorer-link.png "Data Explorer")

2. Select **New Container**.

    ![The button is highlighted.](media/new-container-button.png "New Container")

3. For **Database id**, select **Use existing**, then select **`CustomerProfile` (1)**. Enter **`UserProfileHTAP`** for the **Container id (2)**, then enter **`/userId`** for the **Partition key (3)**. For **Throughput**, select **Autoscale (4)**, then enter **`4000`** for the **Max RU/s** value **(5)**. Finally, set **Analytical store** to **On (6)**, then select **OK**.

    ![The form is configured as described.](media/new-container.png "New container")

    Here we set the `partition key` value to `customerId`, because it is a field we use most often in queries and contains a relatively high cardinality (number of unique values) for good partitioning performance. We set the throughput to Autoscale with a maximum value of 4,000 request units (RUs). This means that the container will have a minimum of 400 RUs allocated (10% of the maximum number), and will scale up to a maximum of 4,000 when the scale engine detects a high enough demand to warrant increasing the throughput. Finally, we enably the **analytical store** on the container, which allows us to take full advantage of the Hybrid Transactional/Analytical Processing (HTAP) architecture from within Synapse Analytics.

    Let's take a quick look at the data we will copy over to the new container.

4. Expand the `OnlineUserProfile01` container underneath the **CustomerProfile** database, then select **Items (1)**. Select one of the documents **(2)** and view its contents **(3)**. The documents are stored in JSON format.

    ![The container items are displayed.](media/existing-items.png "Container items")

### Create and run a copy pipeline

Now that we have the new Azure Cosmos DB container with the analytical store enabled, we need to copy the contents of the existing container by using a Synapse Pipeline.

1. Open Synapse Analytics Studio (<https://web.azuresynapse.net/>), and then navigate to the **Orchestrate** hub.

    ![The Orchestrate menu item is highlighted.](media/orchestrate-hub.png "Orchestrate hub")

2. Select **+ (1)**, then **Pipeline (2)**.

    ![The new pipeline link is highlighted.](media/new-pipeline.png "New pipeline")

3. Under Activities, expand the `Move & transform` group, then drag the **Copy data** activity onto the canvas **(1)**. Set the **Name** to **`Copy Cosmos DB Container`** in the Properties blade **(2)**.

    ![The new copy activity is displayed.](media/add-copy-pipeline.png "Add copy activity")

4. Select the new Copy activity that you added to the canvas, then select the **Source** tab **(1)**. Select the **`asal400_customerprofile_cosmosdb`** source dataset from the list **(2)**.

    ![The source is selected.](media/copy-source.png "Source")

5. Select the **Sink** tab **(1)**, then select **+ New (2)**.

    ![The sink is selected.](media/copy-sink.png "Sink")

6. Select the **Azure Cosmos DB (SQL API)** dataset type **(1)**, then select **Continue (2)**.

    ![Azure Cosmos DB is selected.](media/dataset-type.png "New dataset")

7. For **Name**, enter **`cosmos_db_htap` (1)**. Select the **`asacosmosdb01` (2)** **Linked service**. Select **From connection/store** under **Import schema (3)**, then select **OK (4)**.

    ![The form is configured as described.](media/dataset-properties.png "Set properties")

8. Underneath the new sink dataset you just added, select the **Insert** write behavior.

    ![The sink tab is displayed.](media/sink-insert.png "Sink tab")

9. Select **Publish all**, then **Publish** to save the new pipeline.

    ![Publish all.](media/publish-all-1.png "Publish")

10. Above the pipeline canvas, select **Add trigger (1)**, then **Trigger now (2)**. Select **OK** to trigger the run.

    ![The trigger menu is shown.](media/pipeline-trigger.png "Trigger now")

11. Navigate to the **Monitor** hub.

    ![Monitor hub.](media/monitor-hub.png "Monitor hub")

12. Select **Pipeline runs (1)** and wait until the pipeline run has successfully completed **(2)**. You may have to select **Refresh (3)** a few times.

    ![The pipeline run is shown as successfully completed.](media/pipeline-run-status.png "Pipeline runs")

    > This may take around 4 minutes to complete.

## Querying Azure Cosmos DB with Apache Spark for Synapse Analytics

## Querying Azure Cosmos DB with SQL Serverless for Synapse Analytics
