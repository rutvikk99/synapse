# Build automated data integration pipelines with Azure Synapse Pipelines

- [Build automated data integration pipelines with Azure Synapse Pipelines](#build-automated-data-integration-pipelines-with-azure-synapse-pipelines)
  - [Petabyte-scale ingestion with Azure Synapse Pipelines](#petabyte-scale-ingestion-with-azure-synapse-pipelines)
  - [Code-free transformation at scale with Azure Synapse Pipelines](#code-free-transformation-at-scale-with-azure-synapse-pipelines)
  - [Orchestrate data movement and transformation in Azure Synapse Pipelines](#orchestrate-data-movement-and-transformation-in-azure-synapse-pipelines)

## Petabyte-scale ingestion with Azure Synapse Pipelines

## Code-free transformation at scale with Azure Synapse Pipelines

Tailwind Traders would like code-free options for data engineering tasks. Their motivation is driven by the desire to allow junior-level data engineers who understand the data but do not have a lot of development experience build and maintain data transformation operations. The other driver for this requirement is to reduce fragility caused by complex code with reliance on libraries pinned to specific versions, remove code testing requirements, and improve ease of long-term maintenance.

Given these requirements, you recommend building Mapping Data Flows.

Mapping Data flows are pipeline activities that provide a visual way of specifying how to transform data, through a code-free experience. This feature offers data cleansing, transformation, aggregation, conversion, joins, data copy operations, etc.

Additional benefits

- Cloud scale via Spark execution
- Guided experience to easily build resilient data flows
- Flexibility to transform data per user’s comfort
- Monitor and manage data flows from a single pane of glass

1. Navigate to the **Develop** hub.

    ![The Develop menu item is highlighted.](media/develop-hub.png "Develop hub")

2. Select + then **Data flow** to create a new data flow.

    ![The new data flow link is highlighted.](media/new-data-flow-link.png "New data flow")

3. In the **General** section of the **Profiles** pane of the new data flow, update the **Name** to the following: `asal400_lab2_writeuserprofiledatatoasa`.

4. Select **Add Source** on the data flow canvas.

    ![Select Add Source on the data flow canvas.](media/data-flow-canvas-add-source.png "Add Source")

5. Under **Source settings**, configure the following:

    - **Output stream name**: Enter `EcommerceUserProfiles`.
    - **Source type**: Select `Dataset`.
    - **Dataset**: Select `asal400_ecommerce_userprofiles_source`.

    ![The source settings are configured as described.](media/data-flow-user-profiles-source-settings.png "Source settings")

6. Select the **Source options** tab, then configure the following:

    - **Wildcard paths**: Enter `online-user-profiles-02/*.json`.
    - **JSON Settings**: Expand this section, then select the **Single document** setting. This denotes that each file contains a single JSON document.

    ![The source options are configured as described.](media/data-flow-user-profiles-source-options.png "Source options")

<!-- 7. Select **Data preview** and select **Refresh** to display the data. Select a row under the `topProductPurchases` column to see an expanded view of the array.

    ![The data preview tab is displayed with a sample of the file contents.](media/data-flow-user-profiles-data-preview.png "Data preview") -->

1. Select the **+** to the right of the `EcommerceUserProfiles` source, then select the **Derived Column** schema modifier from the context menu.

    ![The plus sign and Derived Column schema modifier are highlighted.](media/data-flow-user-profiles-new-derived-column.png "New Derived Column")

2. Under **Derived column's settings**, configure the following:

    - **Output stream name**: Enter `userId`.
    - **Incoming stream**: Select `EcommerceUserProfiles`.
    - **Columns**: Provide the following information:

        | Column | Expression | Description |
        | --- | --- | --- |
        | visitorId | `toInteger(visitorId)` | Converts the `visitorId` column from a string to an integer. |

    ![The derived column's settings are configured as described.](media/data-flow-user-profiles-derived-column-settings.png "Derived column's settings")

3. Select the **+** to the right of the `userId` step, then select the **Flatten** schema modifier from the context menu.

    ![The plus sign and the Flatten schema modifier are highlighted.](media/data-flow-user-profiles-new-flatten.png "New Flatten schema modifier")

4. Under **Flatten settings**, configure the following:

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

    These settings provide a flattened view of the data source with one or more rows per `visitorId`, similar to when you explored the data within the Spark notebook in lab 1. Using data preview requires you to enable Debug mode, which we are not enabling for this lab. *The following screenshot is for illustration only*:

    ![The data preview tab is displayed with a sample of the file contents.](media/data-flow-user-profiles-flatten-data-preview.png "Data preview")

5. Select the **+** to the right of the `UserTopProducts` step, then select the **Derived Column** schema modifier from the context menu.

    ![The plus sign and Derived Column schema modifier are highlighted.](media/data-flow-user-profiles-new-derived-column2.png "New Derived Column")

6. Under **Derived column's settings**, configure the following:

    - **Output stream name**: Enter `DeriveProductColumns`.
    - **Incoming stream**: Select `UserTopProducts`.
    - **Columns**: Provide the following information:

        | Column | Expression | Description |
        | --- | --- | --- |
        | productId | `toInteger(productId)` | Converts the `productId` column from a string to an integer. |
        | itemsPurchasedLast12Months | `toInteger(itemsPurchasedLast12Months)` | Converts the `itemsPurchasedLast12Months` column from a string to an integer. |

    ![The derived column's settings are configured as described.](media/data-flow-user-profiles-derived-column2-settings.png "Derived column's settings")

7. Select **Add Source** on the data flow canvas beneath the `EcommerceUserProfiles` source.

    ![Select Add Source on the data flow canvas.](media/data-flow-user-profiles-add-source.png "Add Source")

8. Under **Source settings**, configure the following:

    - **Output stream name**: Enter `UserProfiles`.
    - **Source type**: Select `Dataset`.
    - **Dataset**: Select `asal400_customerprofile_cosmosdb`.

    ![The source settings are configured as described.](media/data-flow-user-profiles-source2-settings.png "Source settings")

9. Since we are not using the data flow debugger, we need to enter the data flow's Script view to update the source projection. Select **Script** in the toolbar above the canvas.

    ![The Script link is highlighted above the canvas.](media/data-flow-user-profiles-script-link.png "Data flow canvas")

10. Locate the **UserProfiles** `source` in the script and replace its script block with the following to set `preferredProducts` as an `integer[]` array and ensure the data types within the `productReviews` array are correctly defined:

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

11. Select **OK** to apply the script changes. The data source has now been updated with the new schema. The following screenshot shows what the source data looks like if you are able to view it with the data preview option. Using data preview requires you to enable Debug mode, which we are not enabling for this lab. *The following screenshot is for illustration only*:

    ![The data preview tab is displayed with a sample of the file contents.](media/data-flow-user-profiles-data-preview2.png "Data preview")

12. Select the **+** to the right of the `UserProfiles` source, then select the **Flatten** schema modifier from the context menu.

    ![The plus sign and the Flatten schema modifier are highlighted.](media/data-flow-user-profiles-new-flatten2.png "New Flatten schema modifier")

13. Under **Flatten settings**, configure the following:

    - **Output stream name**: Enter `UserPreferredProducts`.
    - **Incoming stream**: Select `UserProfiles`.
    - **Unroll by**: Select `[] preferredProducts`.
    - **Input columns**: Provide the following information. Be sure to **delete** `cartId` and `[] productReviews`:

        | UserProfiles's column | Name as |
        | --- | --- |
        | userId | `userId` |
        | [] preferredProducts | `preferredProductId` |

    ![The flatten settings are configured as described.](media/data-flow-user-profiles-flatten2-settings.png "Flatten settings")

    These settings provide a flattened view of the data source with one or more rows per `userId`. Using data preview requires you to enable Debug mode, which we are not enabling for this lab. *The following screenshot is for illustration only*:

    ![The data preview tab is displayed with a sample of the file contents.](media/data-flow-user-profiles-flatten2-data-preview.png "Data preview")

14. Now it is time to join the two data sources. Select the **+** to the right of the `DeriveProductColumns` step, then select the **Join** option from the context menu.

    ![The plus sign and new Join menu item are highlighted.](media/data-flow-user-profiles-new-join.png "New Join")

15. Under **Join settings**, configure the following:

    - **Output stream name**: Enter `JoinTopProductsWithPreferredProducts`.
    - **Left stream**: Select `DeriveProductColumns`.
    - **Right stream**: Select `UserPreferredProducts`.
    - **Join type**: Select `Full outer`.
    - **Join conditions**: Provide the following information:

        | Left: DeriveProductColumns's column | Right: UserPreferredProducts's column |
        | --- | --- |
        | `visitorId` | `userId` |

    ![The join settings are configured as described.](media/data-flow-user-profiles-join-settings.png "Join settings")

16. Select **Optimize** and configure the following:

    - **Broadcast**: Select `Fixed`.
    - **Broadcast options**: Check `Left: 'DeriveProductColumns'`.
    - **Partition option**: Select `Set partitioning`.
    - **Partition type**: Select `Hash`.
    - **Number of partitions**: Enter `30`.
    - **Column**: Select `productId`.

    ![The join optimization settings are configured as described.](media/data-flow-user-profiles-join-optimize.png "Optimize")

    <!-- **TODO**: Add optimization description. -->

17. Select the **Inspect** tab to see the join mapping, including the column feed source and whether the column is used in a join.

    ![The inspect blade is displayed.](media/data-flow-user-profiles-join-inspect.png "Inspect")

    **For illustrative purposes of data preview only:** Since we are not turning on data flow debugging, do not perform this step. In this small sample of data, likely the `userId` and `preferredProductId` columns will only show null values. If you want to get a sense of how many records contain values for these fields, select a column, such as `preferredProductId`, then select **Statistics** in the toolbar above. This displays a chart for the column showing the ratio of values.

    ![The data preview results are shown and the statistics for the preferredProductId column is displayed as a pie chart to the right.](media/data-flow-user-profiles-join-preview.png "Data preview")

18. Select the **+** to the right of the `JoinTopProductsWithPreferredProducts` step, then select the **Derived Column** schema modifier from the context menu.

    ![The plus sign and Derived Column schema modifier are highlighted.](media/data-flow-user-profiles-new-derived-column3.png "New Derived Column")

19. Under **Derived column's settings**, configure the following:

    - **Output stream name**: Enter `DerivedColumnsForMerge`.
    - **Incoming stream**: Select `JoinTopProductsWithPreferredProducts`.
    - **Columns**: Provide the following information (_type in_ the _first two_ column names):

        | Column | Expression | Description |
        | --- | --- | --- |
        | isTopProduct | `toBoolean(iif(isNull(productId), 'false', 'true'))` | Returns `true` if `productId` is not null. Recall that `productId` is fed by the e-commerce top user products data lineage. |
        | isPreferredProduct | `toBoolean(iif(isNull(preferredProductId), 'false', 'true'))` | Returns `true` if `preferredProductId` is not null. Recall that `preferredProductId` is fed by the Azure Cosmos DB user profile data lineage. |
        | productId | `iif(isNull(productId), preferredProductId, productId)` | Sets the `productId` output to either the `preferredProductId` or `productId` value, depending on whether `productId` is null.
        | userId | `iif(isNull(userId), visitorId, userId)` | Sets the `userId` output to either the `visitorId` or `userId` value, depending on whether `userId` is null.

    ![The derived column's settings are configured as described.](media/data-flow-user-profiles-derived-column3-settings.png "Derived column's settings")

    The derived column settings provide the following result:

    ![The data preview is displayed.](media/data-flow-user-profiles-derived-column3-preview.png "Data preview")

20. Select the **+** to the right of the `DerivedColumnsForMerge` step, then select the **Sink** destination from the context menu.

    ![The new Sink destination is highlighted.](media/data-flow-user-profiles-new-sink.png "New sink")

21. Under **Sink**, configure the following:

    - **Output stream name**: Enter `UserTopProductPurchasesASA`.
    - **Incoming stream**: Select `DerivedColumnsForMerge`.
    - **Dataset**: Select `asal400_wwi_usertopproductpurchases_asa`, which is the UserTopProductPurchases SQL table.
    - **Options**: Check `Allow schema drift` and uncheck `Validate schema`.

    ![The sink settings are shown.](media/data-flow-user-profiles-new-sink-settings.png "Sink settings")

22. Select **Settings**, then configure the following:

    - **Update method**: Check `Allow insert` and leave the rest unchecked.
    - **Table action**: Select `Truncate table`.
    - **Enable staging**: `Check` this option. Since we are importing a lot of data, we want to enable staging to improve performance.

    ![The settings are shown.](media/data-flow-user-profiles-new-sink-settings-options.png "Settings")

23. Select **Mapping**, then configure the following:

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

24. Your completed data flow should look similar to the following:

    ![The completed data flow is displayed.](media/data-flow-user-profiles-complete.png "Completed data flow")

25. Select **Publish all** to save your new data flow.

    ![Publish all is highlighted.](media/publish-all-1.png "Publish all")

## Orchestrate data movement and transformation in Azure Synapse Pipelines
