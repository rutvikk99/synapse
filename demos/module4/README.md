# Build automated data integration pipelines with Azure Synapse Pipelines

- [Build automated data integration pipelines with Azure Synapse Pipelines](#build-automated-data-integration-pipelines-with-azure-synapse-pipelines)
  - [Petabyte-scale ingestion with Azure Synapse Pipelines](#petabyte-scale-ingestion-with-azure-synapse-pipelines)
  - [Code-free transformation at scale with Azure Synapse Pipelines](#code-free-transformation-at-scale-with-azure-synapse-pipelines)
    - [Create SQL table](#create-sql-table)
    - [Create linked service](#create-linked-service)
    - [Create data sets](#create-data-sets)
    - [Create Mapping Data Flow](#create-mapping-data-flow)
  - [Orchestrate data movement and transformation in Azure Synapse Pipelines](#orchestrate-data-movement-and-transformation-in-azure-synapse-pipelines)
    - [Create pipeline](#create-pipeline)
    - [Trigger, monitor, and analyze the user profile data pipeline](#trigger-monitor-and-analyze-the-user-profile-data-pipeline)
    - [Create Synapse Spark notebook to find top products](#create-synapse-spark-notebook-to-find-top-products)
    - [Add the Notebook to the pipeline](#add-the-notebook-to-the-pipeline)

## Petabyte-scale ingestion with Azure Synapse Pipelines

## Code-free transformation at scale with Azure Synapse Pipelines

Tailwind Traders would like code-free options for data engineering tasks. Their motivation is driven by the desire to allow junior-level data engineers who understand the data but do not have a lot of development experience build and maintain data transformation operations. The other driver for this requirement is to reduce fragility caused by complex code with reliance on libraries pinned to specific versions, remove code testing requirements, and improve ease of long-term maintenance.

Their other requirement is to maintain transformed data in a data lake in addition to the SQL pool. This gives them the flexibility to retain more fields in their data sets than they otherwise store in fact and dimension tables, and doing this allows them to access the data when they have paused the SQL pool, as a cost optimization.

Given these requirements, you recommend building Mapping Data Flows.

Mapping Data flows are pipeline activities that provide a visual way of specifying how to transform data, through a code-free experience. This feature offers data cleansing, transformation, aggregation, conversion, joins, data copy operations, etc.

Additional benefits

- Cloud scale via Spark execution
- Guided experience to easily build resilient data flows
- Flexibility to transform data per user’s comfort
- Monitor and manage data flows from a single pane of glass

### Create SQL table

The Mapping Data Flow we will build will write user purchase data to a SQL pool. Tailwind Traders does not yet have a table to store this data. We will execute a SQL script to create this table as a pre-requisite.

> **Note to presenter**: Skip this section if you have already created the `[wwi].[UserTopProductPurchases]` table.

1. Open Synapse Studio (<https://web.azuresynapse.net/>).

2. Navigate to the **Develop** hub.

    ![The Develop menu item is highlighted.](media/develop-hub.png "Develop hub")

3. From the **Develop** menu, select the **+** button **(1)** and choose **SQL Script (2)** from the context menu.

    ![The SQL script context menu item is highlighted.](media/synapse-studio-new-sql-script.png "New SQL script")

4. In the toolbar menu, connect to the **SQL Pool** database to execute the query.

    ![The connect to option is highlighted in the query toolbar.](media/synapse-studio-query-toolbar-connect.png "Query toolbar")

5. In the query window, replace the script with the following to create a new table that joins users' preferred products stored in Azure Cosmos DB with top product purchases per user from the e-commerce site, stored in JSON files within the data lake:

    ```sql
    CREATE TABLE [wwi].[UserTopProductPurchases]
    (
        [UserId] [int]  NOT NULL,
        [ProductId] [int]  NOT NULL,
        [ItemsPurchasedLast12Months] [int]  NULL,
        [IsTopProduct] [bit]  NOT NULL,
        [IsPreferredProduct] [bit]  NOT NULL
    )
    WITH
    (
        DISTRIBUTION = HASH ( [UserId] ),
        CLUSTERED COLUMNSTORE INDEX
    )
    ```

6. Select **Run** from the toolbar menu to execute the SQL command.

    ![The run button is highlighted in the query toolbar.](media/synapse-studio-query-toolbar-run.png "Run")

### Create linked service

Azure Cosmos DB is one of the data sources that will be used in the Mapping Data Flow. Tailwind Traders has not yet created the linked service. Follow the steps in this section to create one.

> **Note to presenter**: Skip this section if you have already created a Cosmos DB linked service.

1. Navigate to the **Manage** hub.

    ![The Manage menu item is highlighted.](media/manage-hub.png "Manage hub")

2. Open **Linked services** and select **+ New** to create a new linked service. Select **Azure Cosmos DB (SQL API)** in the list of options, then select **Continue**.

    ![Manage, New, and the Azure Cosmos DB linked service option are highlighted.](media/create-cosmos-db-linked-service-step1.png "New linked service")

3. Name the linked service `asacosmosdb01` **(1)**, select the **Cosmos DB account name** and set the **Database name** value to `CustomerProfile` **(2)**. Select **Test connection** to ensure success **(3)**, then select **Create (4)**.

    ![New Azure Cosmos DB linked service.](media/create-cosmos-db-linked-service.png "New linked service")

### Create data sets

User profile data comes from two different data sources, which we will create now: `asal400_ecommerce_userprofiles_source` and `asal400_customerprofile_cosmosdb`. The customer profile data from an e-commerce system that provides top product purchases for each visitor of the site (customer) over the past 12 months is stored within JSON files in the data lake. User profile data containing, among other things, product preferences and product reviews is stored as JSON documents in Cosmos DB.

In this section, you'll create datasets for the SQL tables that will serve as data sinks for data pipelines you'll create later in this lab.

Complete the steps below to create the following two datasets: `asal400_ecommerce_userprofiles_source` and `asal400_customerprofile_cosmosdb`.

1. Navigate to the **Data** hub.

    ![The Data menu item is highlighted.](media/data-hub.png "Data hub")

2. Select **+** in the toolbar **(1)**, then select **Integration dataset (2)** to create a new dataset.

    ![Create new Dataset.](media/new-dataset.png "New Dataset")

3. Select **Azure Cosmos DB (SQL API)** from the list **(1)**, then select **Continue (2)**.

    ![The Azure Cosmos DB SQL API option is highlighted.](media/new-cosmos-db-dataset.png "Integration dataset")

4. Configure the dataset with the following characteristics, then select **OK (4)**:

    - **Name**: Enter `asal400_customerprofile_cosmosdb` **(1)**.
    - **Linked service**: Select the Azure Cosmos DB linked service **(2)**.
    - **Collection**: Select `OnlineUserProfile01` **(3)**.

    ![New Azure Cosmos DB dataset.](media/create-cosmos-db-dataset.png "New Cosmos DB dataset")

5. After creating the dataset, select **Preview data** under its **Connection** tab.

    ![The preview data button on the dataset is highlighted.](media/cosmos-dataset-preview-data-link.png "Preview data")

6. Preview data queries the selected Azure Cosmos DB collection and returns a sample of the documents within. The documents are stored in JSON format and include a `userId` field, `cartId`, `preferredProducts` (an array of product IDs that may be empty), and `productReviews` (an array of written product reviews that may be empty).

    ![A preview of the Azure Cosmos DB data is displayed.](media/cosmos-db-dataset-preview-data.png "Preview data")

7. Select **+** in the toolbar **(1)**, then select **Integration dataset (2)** to create a new dataset.

    ![Create new Dataset.](media/new-dataset.png "New Dataset")

8. Select **Azure Data Lake Storage Gen2** from the list **(1)**, then select **Continue (2)**.

    ![The ADLS Gen2 option is highlighted.](media/new-adls-dataset.png "Integration dataset")

9. Select the **JSON** format **(1)**, then select **Continue (2)**.

    ![The JSON format is selected.](media/json-format.png "Select format")

10. Configure the dataset with the following characteristics, then select **OK (5)**:

    - **Name**: Enter `asal400_ecommerce_userprofiles_source` **(1)**.
    - **Linked service**: Select the `asadatalakeXX` linked service that already exists **(2)**.
    - **File path**: Browse to the `wwi-02/online-user-profiles-02` path **(3)**.
    - **Import schema**: Select `From connection/store` **(4)**.

    ![The form is configured as described.](media/new-adls-dataset-form.png "Set properties")

11. Select **+** in the toolbar **(1)**, then select **Integration dataset (2)** to create a new dataset.

    ![Create new Dataset.](media/new-dataset.png "New Dataset")

12. Select **Azure Synapse Analytics (formerly SQL DW)** from the list **(1)**, then select **Continue (2)**.

    ![The Azure Synapse Analytics option is highlighted.](media/new-synapse-dataset.png "Integration dataset")

13. Configure the dataset with the following characteristics, then select **OK (5)**:

    - **Name**: Enter `asal400_wwi_usertopproductpurchases_asa` **(1)**.
    - **Linked service**: Select the `SqlPool01` service **(2)**.
    - **Table name**: Select `wwi.UserTopProductPurchases` **(3)**.
    - **Import schema**: Select `From connection/store` **(4)**.

    ![New dataset form is displayed with the described configuration.](media/new-dataset-usertopproductpurchases.png "New dataset")

14. Select **Publish all** then **Publish** to save your new resources.

    ![Publish all is highlighted.](media/publish-all-1.png "Publish all")

### Create Mapping Data Flow

1. Navigate to the **Develop** hub.

    ![The Develop menu item is highlighted.](media/develop-hub.png "Develop hub")

2. Select **+** then **Data flow** to create a new data flow.

    ![The new data flow link is highlighted.](media/new-data-flow-link.png "New data flow")

3. In the **General** section of the **Profiles** pane of the new data flow, update the **Name** to the following: `write_user_profile_to_asa`.

    ![The name is displayed.](media/data-flow-general.png "General properties")

4. Select the **Properties** button to hide the pane.

    ![The button is highlighted.](media/data-flow-properties-button.png "Properties button")

5. Select **Add Source** on the data flow canvas.

    ![Select Add Source on the data flow canvas.](media/data-flow-canvas-add-source.png "Add Source")

6. Under **Source settings**, configure the following:

    - **Output stream name**: Enter `EcommerceUserProfiles`.
    - **Source type**: Select `Dataset`.
    - **Dataset**: Select `asal400_ecommerce_userprofiles_source`.

    ![The source settings are configured as described.](media/data-flow-user-profiles-source-settings.png "Source settings")

7. Select the **Source options** tab, then configure the following:

    - **Wildcard paths**: Enter `online-user-profiles-02/*.json`.
    - **JSON Settings**: Expand this section, then select the **Single document** setting. This denotes that each file contains a single JSON document.

    ![The source options are configured as described.](media/data-flow-user-profiles-source-options.png "Source options")

8. Select the **+** to the right of the `EcommerceUserProfiles` source, then select the **Derived Column** schema modifier from the context menu.

    ![The plus sign and Derived Column schema modifier are highlighted.](media/data-flow-user-profiles-new-derived-column.png "New Derived Column")

9. Under **Derived column's settings**, configure the following:

    - **Output stream name**: Enter `userId`.
    - **Incoming stream**: Select `EcommerceUserProfiles`.
    - **Columns**: Provide the following information:

        | Column | Expression | Description |
        | --- | --- | --- |
        | visitorId | `toInteger(visitorId)` | Converts the `visitorId` column from a string to an integer. |

    ![The derived column's settings are configured as described.](media/data-flow-user-profiles-derived-column-settings.png "Derived column's settings")

10. Select the **+** to the right of the `userId` step, then select the **Flatten** schema modifier from the context menu.

    ![The plus sign and the Flatten schema modifier are highlighted.](media/data-flow-user-profiles-new-flatten.png "New Flatten schema modifier")

11. Under **Flatten settings**, configure the following:

    - **Output stream name**: Enter `UserTopProducts`.
    - **Incoming stream**: Select `userId`.
    - **Unroll by**: Select `[] topProductPurchases`.
    - **Input columns**: Provide the following information:

        | userId's column | Name as |
        | --- | --- |
        | visitorId | `visitorId` |
        | topProductPurchases.productId | `productId` |
        | topProductPurchases.itemsPurchasedLast12Months | `itemsPurchasedLast12Months` |

    ![The flatten settings are configured as described.](media/data-flow-user-profiles-flatten-settings.png "Flatten settings")

    These settings provide a flattened view of the data source with one or more rows per `visitorId`, similar to when we explored the data within the Spark notebook in the previous module. Using data preview requires you to enable Debug mode, which we are not enabling for this lab. *The following screenshot is for illustration only*:

    ![The data preview tab is displayed with a sample of the file contents.](media/data-flow-user-profiles-flatten-data-preview.png "Data preview")

12. Select the **+** to the right of the `UserTopProducts` step, then select the **Derived Column** schema modifier from the context menu.

    ![The plus sign and Derived Column schema modifier are highlighted.](media/data-flow-user-profiles-new-derived-column2.png "New Derived Column")

13. Under **Derived column's settings**, configure the following:

    - **Output stream name**: Enter `DeriveProductColumns`.
    - **Incoming stream**: Select `UserTopProducts`.
    - **Columns**: Provide the following information:

        | Column | Expression | Description |
        | --- | --- | --- |
        | productId | `toInteger(productId)` | Converts the `productId` column from a string to an integer. |
        | itemsPurchasedLast12Months | `toInteger(itemsPurchasedLast12Months)` | Converts the `itemsPurchasedLast12Months` column from a string to an integer. |

    ![The derived column's settings are configured as described.](media/data-flow-user-profiles-derived-column2-settings.png "Derived column's settings")

14. Select **Add Source** on the data flow canvas beneath the `EcommerceUserProfiles` source.

    ![Select Add Source on the data flow canvas.](media/data-flow-user-profiles-add-source.png "Add Source")

15. Under **Source settings**, configure the following:

    - **Output stream name**: Enter `UserProfiles`.
    - **Source type**: Select `Dataset`.
    - **Dataset**: Select `asal400_customerprofile_cosmosdb`.

    ![The source settings are configured as described.](media/data-flow-user-profiles-source2-settings.png "Source settings")

16. Since we are not using the data flow debugger, we need to enter the data flow's Script view to update the source projection. Select **Script** in the toolbar above the canvas.

    ![The Script link is highlighted above the canvas.](media/data-flow-user-profiles-script-link.png "Data flow canvas")

17. Locate the **UserProfiles** `source` in the script and replace its script block with the following to set `preferredProducts` as an `integer[]` array and ensure the data types within the `productReviews` array are correctly defined:

    ```json
    source(output(
            cartId as string,
            preferredProducts as integer[],
            productReviews as (productId as integer, reviewDate as string, reviewText as string)[],
            userId as integer
        ),
        allowSchemaDrift: true,
        validateSchema: false,
        format: 'document') ~> UserProfiles
    ```

    ![The script view is displayed.](media/data-flow-user-profiles-script.png "Script view")

18. Select **OK** to apply the script changes. The data source has now been updated with the new schema. The following screenshot shows what the source data looks like if you are able to view it with the data preview option. Using data preview requires you to enable Debug mode, which we are not enabling for this lab. *The following screenshot is for illustration only*:

    ![The data preview tab is displayed with a sample of the file contents.](media/data-flow-user-profiles-data-preview2.png "Data preview")

19. Select the **+** to the right of the `UserProfiles` source, then select the **Flatten** schema modifier from the context menu.

    ![The plus sign and the Flatten schema modifier are highlighted.](media/data-flow-user-profiles-new-flatten2.png "New Flatten schema modifier")

20. Under **Flatten settings**, configure the following:

    - **Output stream name**: Enter `UserPreferredProducts`.
    - **Incoming stream**: Select `UserProfiles`.
    - **Unroll by**: Select `[] preferredProducts`.
    - **Input columns**: Provide the following information. Be sure to **delete** `cartId` and `[] productReviews`:

        | UserProfiles's column | Name as |
        | --- | --- |
        | [] preferredProducts | `preferredProductId` |
        | userId | `userId` |

    ![The flatten settings are configured as described.](media/data-flow-user-profiles-flatten2-settings.png "Flatten settings")

    These settings provide a flattened view of the data source with one or more rows per `userId`. Using data preview requires you to enable Debug mode, which we are not enabling for this lab. *The following screenshot is for illustration only*:

    ![The data preview tab is displayed with a sample of the file contents.](media/data-flow-user-profiles-flatten2-data-preview.png "Data preview")

21. Now it is time to join the two data sources. Select the **+** to the right of the `DeriveProductColumns` step, then select the **Join** option from the context menu.

    ![The plus sign and new Join menu item are highlighted.](media/data-flow-user-profiles-new-join.png "New Join")

22. Under **Join settings**, configure the following:

    - **Output stream name**: Enter `JoinTopProductsWithPreferredProducts`.
    - **Left stream**: Select `DeriveProductColumns`.
    - **Right stream**: Select `UserPreferredProducts`.
    - **Join type**: Select `Full outer`.
    - **Join conditions**: Provide the following information:

        | Left: DeriveProductColumns's column | Right: UserPreferredProducts's column |
        | --- | --- |
        | `visitorId` | `userId` |

    ![The join settings are configured as described.](media/data-flow-user-profiles-join-settings.png "Join settings")

23. Select **Optimize** and configure the following:

    - **Broadcast**: Select `Fixed`.
    - **Broadcast options**: Check `Left: 'DeriveProductColumns'`.
    - **Partition option**: Select `Set partitioning`.
    - **Partition type**: Select `Hash`.
    - **Number of partitions**: Enter `30`.
    - **Column**: Select `productId`.

    ![The join optimization settings are configured as described.](media/data-flow-user-profiles-join-optimize.png "Optimize")

    <!-- **TODO**: Add optimization description. -->

24. Select the **Inspect** tab to see the join mapping, including the column feed source and whether the column is used in a join.

    ![The inspect blade is displayed.](media/data-flow-user-profiles-join-inspect.png "Inspect")

    **For illustrative purposes of data preview only:** Since we are not turning on data flow debugging, do not perform this step. In this small sample of data, likely the `userId` and `preferredProductId` columns will only show null values. If you want to get a sense of how many records contain values for these fields, select a column, such as `preferredProductId`, then select **Statistics** in the toolbar above. This displays a chart for the column showing the ratio of values.

    ![The data preview results are shown and the statistics for the preferredProductId column is displayed as a pie chart to the right.](media/data-flow-user-profiles-join-preview.png "Data preview")

25. Select the **+** to the right of the `JoinTopProductsWithPreferredProducts` step, then select the **Derived Column** schema modifier from the context menu.

    ![The plus sign and Derived Column schema modifier are highlighted.](media/data-flow-user-profiles-new-derived-column3.png "New Derived Column")

26. Under **Derived column's settings**, configure the following:

    - **Output stream name**: Enter `DerivedColumnsForMerge`.
    - **Incoming stream**: Select `JoinTopProductsWithPreferredProducts`.
    - **Columns**: Provide the following information (**_type in_ the _first two_ column names**):

        | Column | Expression | Description |
        | --- | --- | --- |
        | isTopProduct | `toBoolean(iif(isNull(productId), 'false', 'true'))` | Returns `true` if `productId` is not null. Recall that `productId` is fed by the e-commerce top user products data lineage. |
        | isPreferredProduct | `toBoolean(iif(isNull(preferredProductId), 'false', 'true'))` | Returns `true` if `preferredProductId` is not null. Recall that `preferredProductId` is fed by the Azure Cosmos DB user profile data lineage. |
        | productId | `iif(isNull(productId), preferredProductId, productId)` | Sets the `productId` output to either the `preferredProductId` or `productId` value, depending on whether `productId` is null.
        | userId | `iif(isNull(userId), visitorId, userId)` | Sets the `userId` output to either the `visitorId` or `userId` value, depending on whether `userId` is null.

    ![The derived column's settings are configured as described.](media/data-flow-user-profiles-derived-column3-settings.png "Derived column's settings")

    The derived column settings provide the following result:

    ![The data preview is displayed.](media/data-flow-user-profiles-derived-column3-preview.png "Data preview")

27. Select the **+** to the right of the `DerivedColumnsForMerge` step, then select the **Filter** destination from the context menu.

    ![The new Filter destination is highlighted.](media/data-flow-user-profiles-new-filter.png "New filter")

    We are adding the Filter step to remove any records where the `ProductId` is null. The data sets have a small percentage of invalid records, and null `ProductId` values will cause errors when loading into the `UserTopProductPurchases` SQL pool table.

28. Set the **Filter on** expression to **`!isNull(productId)`**.

    ![The filter settings are shown.](media/data-flow-user-profiles-new-filter-settings.png "Filter settings")

29. Select the **+** to the right of the `Filter1` step, then select the **Sink** destination from the context menu.

    ![The new Sink destination is highlighted.](media/data-flow-user-profiles-new-sink.png "New sink")

30. Under **Sink**, configure the following:

    - **Output stream name**: Enter `UserTopProductPurchasesASA`.
    - **Incoming stream**: Select `Filter1`.
    - **Sink type**: select `Dataset`.
    - **Dataset**: Select `asal400_wwi_usertopproductpurchases_asa`, which is the UserTopProductPurchases SQL table.
    - **Options**: Check `Allow schema drift` and uncheck `Validate schema`.

    ![The sink settings are shown.](media/data-flow-user-profiles-new-sink-settings.png "Sink settings")

31. Select **Settings**, then configure the following:

    - **Update method**: Check `Allow insert` and leave the rest unchecked.
    - **Table action**: Select `Truncate table`.
    - **Enable staging**: `Check` this option. Since we are importing a lot of data, we want to enable staging to improve performance.

    ![The settings are shown.](media/data-flow-user-profiles-new-sink-settings-options.png "Settings")

32. Select **Mapping**, then configure the following:

    - **Auto mapping**: `Uncheck` this option.
    - **Columns**: Provide the following information:

        | Input columns | Output columns |
        | --- | --- |
        | userId | UserId |
        | productId | ProductId |
        | itemsPurchasedLast12Months | ItemsPurchasedLast12Months |
        | isTopProduct | IsTopProduct |
        | isPreferredProduct | IsPreferredProduct |

    ![The mapping settings are configured as described.](media/data-flow-user-profiles-new-sink-settings-mapping.png "Mapping")

33. Select the **+** to the right of the `Filter1` step, then select the **Sink** destination from the context menu to add a second sink.

    ![The new Sink destination is highlighted.](media/data-flow-user-profiles-new-sink2.png "New sink")

34. Under **Sink**, configure the following:

    - **Output stream name**: Enter `DataLake`.
    - **Incoming stream**: Select `Filter1`.
    - **Sink type**: select `Delta`.
    - **Linked service**: Select the default workspace data lake storage account (example: `asaworkspaceinaday84-WorspaceDefaultStorage`).
    - **Options**: Check `Allow schema drift` and uncheck `Validate schema`.

    ![The sink settings are shown.](media/data-flow-user-profiles-new-sink-settings2.png "Sink settings")

35. Select **Settings**, then configure the following:

    - **Folder path**: Enter `wwi-02/top-products` (**copy and paste** these two values into the fields since the `top-products` folder does not yet exist).
    - **Compression type**: Select `snappy`.
    - **Compression level**: Select `Fastest`.
    - **Vacuum**: Enter 0.
    - **Truncate table**: Check.
    - **Merge schema**: Unchecked.
    - **Update method**: Check `Allow insert` and leave the rest unchecked.

    ![The settings are shown.](media/data-flow-user-profiles-new-sink-settings-options2.png "Settings")

36. Select **Mapping**, then configure the following:

    - **Auto mapping**: `Uncheck` this option.
    - **Columns**: Provide the following information:

        | Input columns | Output columns |
        | --- | --- |
        | visitorId | visitorId |
        | productId | ProductId |
        | itemsPurchasedLast12Months | ItemsPurchasedLast12Months |
        | preferredProductId | preferredProductId |
        | userId | UserId |
        | isTopProduct | IsTopProduct |
        | isPreferredProduct | IsPreferredProduct |

    ![The mapping settings are configured as described.](media/data-flow-user-profiles-new-sink-settings-mapping2.png "Mapping")

    > Notice that we have chosen to keep more fields for the data lake sink vs. the SQL pool sink (`visitorId` and `preferredProductId`). This is because we aren't adhering to a fixed destination schema (like a SQL table), and because we want to retain the original data as much as possible in the data lake.

37. Your completed data flow should look similar to the following:

    ![The completed data flow is displayed.](media/data-flow-user-profiles-complete.png "Completed data flow")

38. Select **Publish all** to save your new data flow.

    ![Publish all is highlighted.](media/publish-all-1.png "Publish all")

## Orchestrate data movement and transformation in Azure Synapse Pipelines

Tailwind Traders is familiar with Azure Data Factory (ADF) pipelines and wants to know if Azure Synapse Analytics can either integrate with ADF or has a similar capability. They want to orchestrate data ingest, transformation, and load activities across their entire data catalog, both internal and external to their data warehouse.

You recommend using Synapse Pipelines, which includes over 90 built-in connectors, can load data by manual execution of the pipeline or by orchestration, supports common loading patterns, enables fully parallel loading into the data lake or SQL tables, and shares a code base with ADF.

By using Synapse Pipelines, Tailwind Traders can experience the same familiar interface as ADF without having to use an orchestration service outside of Synapse Analytics.

### Create pipeline

Let's start by executing our new Mapping Data Flow. In order to run the new data flow, we need to create a new pipeline and add a data flow activity to it.

1. Navigate to the **Orchestrate** hub.

    ![The Orchestrate hub is highlighted.](media/orchestrate-hub.png "Orchestrate hub")

2. Select **+ (1)**, then **Pipeline (2)**.

    ![The new pipeline menu item is highlighted.](media/new-pipeline.png "New pipeline")

3. In the **General** section of the **Profiles** pane of the new data flow, update the **Name** to the following: `Write User Profile Data to ASA`.

    ![The name is displayed.](media/pipeline-general.png "General properties")

4. Select the **Properties** button to hide the pane.

    ![The button is highlighted.](media/pipeline-properties-button.png "Properties button")

5. Expand **Move & transform** within the Activities list, then drag the **Data flow** activity onto the pipeline canvas.

    ![Drag the data flow activity onto the pipeline canvas.](media/pipeline-drag-data-flow.png "Pipeline canvas")

6. In the `Adding data flow` blade, select **Use existing data flow (1)**, then select the `write_user_profile_to_asa` existing data flow **(2)** you created in the previous task.

    ![The adding data flow form is displayed with the described configuration.](media/pipeline-user-profiles-adding-data-flow.png "Adding data flow")

7. Select **OK (3)**.

8. Select the mapping data flow activity on the canvas. Select the **Settings** tab **(1)**, then ensure `AutoResolveIntegrationRuntime` is selected for **Run on (Azure IR) (2)**. Choose the `Compute Optimized` **Compute type (3)** and select `64 (+ 16 cores)` for the **Core count (4)**.

    ![The settings are configured as described.](media/data-flow-activity-settings1.png "Settings")

9. Expand **PolyBase** and configure the following:

    - **Staging linked service**: Select the `asadatalakeSUFFIX` linked service.
    - **Staging storage folder**: Enter `staging/userprofiles`. The `userprofiles` folder will be automatically created for you during the first pipeline run.

    > **Copy and paste** the `staging` and `userprofiles` folder names into the two fields.

    ![The mapping data flow activity settings are configured as described.](media/pipeline-user-profiles-data-flow-settings.png "Mapping data flow activity settings")

    The staging options under PolyBase is recommended when you have a large amount of data to move into or out of Azure Synapse Analytics. You will want to experiment with enabling and disabling staging on the data flow in a production environment to evaluate the difference in performance.

10. Select **Publish all** then **Publish** to save your pipeline.

    ![Publish all is highlighted.](media/publish-all-1.png "Publish all")

### Trigger, monitor, and analyze the user profile data pipeline

Tailwind Traders wants to monitor all pipeline runs and view statistics for performance tuning and troubleshooting purposes.

You have decided to show Tailwind Traders how to manually trigger, monitor, then analyze a pipeline run.

1. At the top of the pipeline, select **Add trigger (1)**, then **Trigger now (2)**.

    ![The pipeline trigger option is highlighted.](media/pipeline-user-profiles-trigger.png "Trigger now")

2. There are no parameters for this pipeline, so select **OK** to run the trigger.

    ![The OK button is highlighted.](media/pipeline-run-trigger.png "Pipeline run")

3. Navigate to the **Monitor** hub.

    ![The Monitor hub menu item is selected.](media/monitor-hub.png "Monitor hub")

4. Select **Pipeline runs (1)** and wait for the pipeline run to successfully complete **(2)**. You may need to refresh **(3)** the view.

    ![The pipeline run succeeded.](media/pipeline-user-profiles-run-complete.png "Pipeline runs")

5. Select the name of the pipeline to view the pipeline's activity runs.

    ![The pipeline name is selected.](media/select-pipeline.png "Pipeline runs")

6. Hover over the data flow activity name in the `Activity runs` list, then select the **Data flow details** icon.

    ![The data flow details icon is highlighted.](media/pipeline-user-profiles-activity-runs.png "Activity runs")

7. The data flow details displays the data flow steps and processing details. In our example, processing time took around **44 seconds to process** the SQL pool sink **(1)**, and around **12 seconds to process** the Data Lake sink **(2)**. The Filter1 output was around **1 million rows (3)** for both. You can see which activities took the longest to complete. The cluster startup time contributed over **2.5 minutes (4)** to the total pipeline run.

    ![The data flow details are displayed.](media/pipeline-user-profiles-data-flow-details.png "Data flow details")

8. Select the `UserTopProductPurchasesASA` sink **(1)** to view its details. We can see that **1,622,203 rows** were calculated **(2)** with a total of 30 partitions. It took around **eight seconds** to stage the data **(3)** in ADLS Gen2 prior to writing the data to the SQL table. The total sink processing time in our case was around **44 seconds (4)**. It is also apparent that we have a **hot partition (5)** that is significantly larger than the others. If we need to squeeze extra performance out of this pipeline, we can re-evaluate data partitioning to more evenly spread the partitions to better facilitate parallel data loading and filtering. We could also experiment with disabling staging to see if there's a processing time difference. Finally, the size of the SQL Pool plays a factor in how long it takes to ingest data into the sink.

    ![The sink details are displayed.](media/pipeline-user-profiles-data-flow-sink-details.png "Sink details")

### Create Synapse Spark notebook to find top products

Now that we have processed, joined, and imported the user profile data, let's analyze it in greater detail. In this segment, we will execute code to find the top 5 products for each user, based on which ones are both preferred and top, and have the most purchases in past 12 months. Then, we will calculate the top 5 products overall.

> We will access the data from the data lake that was added as a second sink in the data flow, removing the SQL pool dependency.

1. Navigate to the **Data** hub.

    ![The Data menu item is highlighted.](media/data-hub.png "Data hub")

2. Select the **Linked** tab **(1)** and expand the **primary data lake storage account (2)** underneath the **Azure Data Lake Storage Gen2**. Select the **wwi-02** container **(3)** and open the **top-products** folder **(4)**. Right-click on any Parquet file **(5)**, then select the **New notebook** menu item **(6)**. If you don't see the folder, select `Refresh` above.

    ![The Parquet file and new notebook option are highlighted.](media/synapse-studio-top-products-folder.png "New notebook")

3. Make sure the notebook is attached to your Spark pool.

    ![The attach to Spark pool menu item is highlighted.](media/notebook-top-products-attach-pool.png "Select Spark pool")

4. Replace the Parquet file name with `*.parquet` **(1)** to select all Parquet files in the `top-products` folder. For example, the path should be similar to: `abfss://wwi-02@YOUR_DATALAKE_NAME.dfs.core.windows.net/top-products/*.parquet`.

    ![The filename is highlighted.](media/notebook-top-products-filepath.png "Folder path")

5. Select **Run all** on the notebook toolbar to execute the notebook.

    ![The cell results are displayed.](media/notebook-top-products-cell1results.png "Cell 1 results")

    > **Note:** The first time you run a notebook in a Spark pool, Synapse creates a new session. This can take approximately 3-5 minutes.

    > **Note:** To run just the cell, either hover over the cell and select the _Run cell_ icon to the left of the cell, or select the cell then type **Ctrl+Enter** on your keyboard.

6. Create a new cell underneath by selecting **{} Add code** when hovering over the blank space at the bottom of the notebook.

    ![The Add Code menu option is highlighted.](media/new-cell.png "Add code")

7. Enter and execute the following in the new cell to populate a new dataframe called `topPurchases`, create a new temporary view named `top_purchases`, and show the first 100 rows:

    ```python
        topPurchases = data_path.select(
        "UserId", "ProductId",
        "ItemsPurchasedLast12Months", "IsTopProduct",
        "IsPreferredProduct")

    # Populate a temporary view so we can query from SQL
    topPurchases.createOrReplaceTempView("top_purchases")

    topPurchases.show(100)
    ```

    The output should look similar to the following:

    ```text
    +------+---------+--------------------------+------------+------------------+
    |UserId|ProductId|ItemsPurchasedLast12Months|IsTopProduct|IsPreferredProduct|
    +------+---------+--------------------------+------------+------------------+
    |   148|     2717|                      null|       false|              true|
    |   148|     4002|                      null|       false|              true|
    |   148|     1716|                      null|       false|              true|
    |   148|     4520|                      null|       false|              true|
    |   148|      951|                      null|       false|              true|
    |   148|     1817|                      null|       false|              true|
    |   463|     2634|                      null|       false|              true|
    |   463|     2795|                      null|       false|              true|
    |   471|     1946|                      null|       false|              true|
    |   471|     4431|                      null|       false|              true|
    |   471|      566|                      null|       false|              true|
    |   471|     2179|                      null|       false|              true|
    |   471|     3758|                      null|       false|              true|
    |   471|     2434|                      null|       false|              true|
    |   471|     1793|                      null|       false|              true|
    |   471|     1620|                      null|       false|              true|
    |   471|     1572|                      null|       false|              true|
    |   833|      957|                      null|       false|              true|
    |   833|     3140|                      null|       false|              true|
    |   833|     1087|                      null|       false|              true|
    ```

8. Execute the following in a new cell to create a new DataFrame to hold only top preferred products where both `IsTopProduct` and `IsPreferredProduct` are true:

    ```python
    from pyspark.sql.functions import *

    topPreferredProducts = (topPurchases
        .filter( col("IsTopProduct") == True)
        .filter( col("IsPreferredProduct") == True)
        .orderBy( col("ItemsPurchasedLast12Months").desc() ))

    topPreferredProducts.show(100)
    ```

    ![The cell code and output are displayed.](media/notebook-top-products-top-preferred-df.png "Notebook cell")

9. Execute the following in a new cell to create a new temporary view by using SQL:

    ```sql
    %%sql

    CREATE OR REPLACE TEMPORARY VIEW top_5_products
    AS
        select UserId, ProductId, ItemsPurchasedLast12Months
        from (select *,
                    row_number() over (partition by UserId order by ItemsPurchasedLast12Months desc) as seqnum
            from top_purchases
            ) a
        where seqnum <= 5 and IsTopProduct == true and IsPreferredProduct = true
        order by a.UserId
    ```

    *Note that there is no output for the above query.* The query uses the `top_purchases` temporary view as a source and applies a `row_number() over` method to apply a row number for the records for each user where `ItemsPurchasedLast12Months` is greatest. The `where` clause filters the results so we only retrieve up to five products where both `IsTopProduct` and `IsPreferredProduct` are set to true. This gives us the top five most purchased products for each user where those products are _also_ identified as their favorite products, according to their user profile stored in Azure Cosmos DB.

10. Execute the following in a new cell to create and display a new DataFrame that stores the results of the `top_5_products` temporary view you created in the previous cell:

    ```python
    top5Products = sqlContext.table("top_5_products")

    top5Products.show(100)
    ```

    You should see an output similar to the following, which displays the top five preferred products per user:

    ![The top five preferred products are displayed per user.](media/notebook-top-products-top-5-preferred-output.png "Top 5 preferred products")

11. Execute the following in a new cell to compare the number of top preferred products to the top five preferred products per customer:

    ```python
    print('before filter: ', topPreferredProducts.count(), ', after filter: ', top5Products.count())
    ```

    The output should be similar to `before filter:  997873 , after filter:  85020`.

12. Calculate the top five products overall, based on those that are both preferred by customers and purchased the most. To do this, execute the following in a new cell:

    ```python
    top5ProductsOverall = (top5Products.select("ProductId","ItemsPurchasedLast12Months")
        .groupBy("ProductId")
        .agg( sum("ItemsPurchasedLast12Months").alias("Total") )
        .orderBy( col("Total").desc() )
        .limit(5))

    top5ProductsOverall.show()
    ```

    In this cell, we grouped the top five preferred products by product ID, summed up the total items purchased in the last 12 months, sorted that value in descending order, and returned the top five results. Your output should be similar to the following:

    ```text
    +---------+-----+
    |ProductId|Total|
    +---------+-----+
    |     2107| 4538|
    |     4833| 4533|
    |      347| 4523|
    |     3459| 4233|
    |     4246| 4155|
    +---------+-----+
    ```

13. We are going to execute this notebook from a pipeline. We want to pass in a parameter that sets a `runId` variable value that will be used to name the Parquet file. Execute the following in a new cell:

    ```python
    import uuid

    # Generate random GUID
    runId = uuid.uuid4()
    ```

    We are using the `uuid` library that comes with Spark to generate a random GUID. We want to override the `runId` variable with a parameter passed in by the pipeline. To do this, we need to toggle this as a parameter cell.

14. Select the actions ellipses **(...)** on the top-right corner of the cell **(1)**, then select **Toggle parameter cell (2)**.

    ![The menu item is highlighted.](media/toggle-parameter-cell.png "Toggle parameter cell")

    After toggling this option, you will see the **Parameters** tag on the cell.

    ![The cell is configured to accept parameters.](media/parameters-tag.png "Parameters")

15. Paste the following code in a new cell to use the `runId` variable as the Parquet filename in the `/top5-products/` path in the primary data lake account. **Replace `YOUR_DATALAKE_NAME`** in the path with the name of your primary data lake account. To find this, scroll up to **Cell 1** at the top of the page **(1)**. Copy the data lake storage account from the path **(2)**. Paste this value as a replacement for **`YOUR_DATALAKE_NAME`** in the path **(3)** inside the new cell, then execute the cell.

    ```python
    %%pyspark

    top5ProductsOverall.write.parquet('abfss://wwi-02@YOUR_DATALAKE_NAME.dfs.core.windows.net/top5-products/' + str(runId) + '.parquet')
    ```

    ![The path is updated with the name of the primary data lake account.](media/datalake-path-in-cell.png "Data lake name")

16. Verify that the file was written to the data lake. Navigate to the **Data** hub and select the **Linked** tab **(1)**. Expand the primary data lake storage account and select the **wwi-02** container **(2)**. Navigate to the **top5-products** folder **(3)**. You should see a folder for the Parquet file in the directory with a GUID as the file name **(4)**.

    ![The parquet file is highlighted.](media/top5-products-parquet.png "Top 5 products parquet")

    The Parquet write method on the dataframe in the Notebook cell created this directory since it did not previously exist.

### Add the Notebook to the pipeline

Tailwind Traders wants to execute this notebook after the Mapping Data Flow runs as part of their orchestration process. To do this, we will add this notebook to our pipeline as a new Notebook activity.

1. Return to the notebook. Select the **Properties** button **(1)** at the top-right corner of the notebook, then enter `Calculate Top 5 Products` for the **Name (2)**.

    ![The properties blade is displayed.](media/notebook-top-products-top-5-preferred-properties.png "Properties")

2. Select the **Add to pipeline** button **(1)** at the top-right corner of the notebook, then select **Existing pipeline (2)**.

    ![The add to pipeline button is highlighted.](media/add-to-pipeline.png "Add to pipeline")

3. Select the **Write User Profile Data to ASA** pipeline **(1)**, then select **Add *2)**.

    ![The pipeline is selected.](media/add-to-pipeline-selection.png "Add to pipeline")

4. Synapse Studio adds the Notebook activity to the pipeline. Rearrange the **Notebook activity** so it sits to the right of the **Data flow activity**. Select the **Data flow activity** and drag a **Success** activity pipeline connection **green box** to the **Notebook activity**.

    ![The green arrow is highlighted.](media/success-activity.png "Success activity")

    The Success activity arrow instructs the pipeline to execute the Notebook activity after the Data flow activity successfully runs.

5. Select the **Notebook activity (1)**, select the **Settings** tab **(2)**, expand **Base parameters (3)**, and select **+ New (4)**. Enter **`runId`** in the **Name** field **(5)**. Select **String** for the **Type (6)**. For the **Value**, select **Add dynamic content (7)**.

    ![The settings are displayed.](media/notebook-activity-settings.png "Settings")

6. Select **Pipeline run ID** under **System variables (1)**. This adds `@pipeline().RunId` to the dynamic content box **(2)**. Select **Finish (3)** to close the dialog.

    ![The dynamic content form is displayed.](media/add-dynamic-content.png "Add dynamic content")

    The Pipeline run ID value is a unique GUID assigned to each pipeline run. We will use this value for the name of the Parquet file by passing this value in as the `runId` Notebook parameter. We can then look through the pipeline run history and find the specific Parquet file created for each pipeline run.

7. Select **Publish all** then **Publish** to save your changes.

    ![Publish all is highlighted.](media/publish-all-1.png "Publish all")

8. **OPTIONAL - Pipeline run now takes >10 minutes -** After publishing is complete, select **Add trigger (1)**, then **Trigger now (2)** to run the updated pipeline.

    ![The trigger menu item is highlighted.](media/trigger-updated-pipeline.png "Trigger pipeline")

9. Select **OK** to run the trigger.

    ![The OK button is highlighted.](media/pipeline-run-trigger.png "Pipeline run")

10. Navigate to the **Monitor** hub.

    ![The Monitor hub menu item is selected.](media/monitor-hub.png "Monitor hub")

11. Select **Pipeline runs (1)** and wait for the pipeline run to successfully complete **(2)**. You may need to refresh **(3)** the view.

    ![The pipeline run succeeded.](media/pipeline-user-profiles-run-complete.png "Pipeline runs")

12. Select the name of the pipeline to view the pipeline's activity runs.

    ![The pipeline name is selected.](media/select-pipeline.png "Pipeline runs")

13. This time, we see both the **Data flow** activity, and the new **Notebook** activity **(1)**. Make note of the **Pipeline run ID** value **(2)**. We will compare this to the Parquet file name generated by the notebook. Select the **Calculate Top 5 Products** Notebook name to view its details **(3)**.

    ![The pipeline run details are displayed.](media/pipeline-run-details2.png "Write User Profile Data to ASA details")

14. Here we see the Notebook run details. You can select the **Playback** button **(1)** to watch a playback of the progress through the **jobs (2)**. At the bottom, you can view the **log output** with different filter options **(3)**. To the right, we can view the run details, such as the duration, Livy ID, Spark pool details, etc. Select a **job** to view its details **(5)**.

    ![The run details are displayed.](media/notebook-run-details.png "Notebook run details")

15. The Spark UI opens in a new tab where we can see the stage details. Expand the **DAG Visualization** to view the stage details.

    ![The Spark stage details are displayed.](media/spark-stage-details.png "Stage details")

16. Navigate back to the **Data** hub.

    ![Data hub.](media/data-hub.png "Data hub")

17. Select the **Linked** tab **(1)**, select the **wwi-02** container **(2)** on the primary data lake storage account, navigate to the **top5-products** folder **(3)**, and verify that a folder exists for the Parquet file whose name matches the **Pipeline run ID**.

    ![The file is highlighted.](media/parquet-from-pipeline-run.png "Parquet file from pipeline run")

    As you can see, we have a file whose name matches the **Pipeline run ID** we noted earlier:

    ![The Pipeline run ID is highlighted.](media/pipeline-run-id.png "Pipeline run ID")

    These values match because we passed in the Pipeline run ID to the `runId` parameter on the Notebook activity.
