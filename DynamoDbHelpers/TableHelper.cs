using System.Net;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDbHelpers;

public static class TableHelper
{
    public static async Task<bool> CreateTable(AmazonDynamoDBClient client, string tableName, string primaryKeyColumnName, string sortKeyColumnName)
    {
        var response = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = tableName,
            AttributeDefinitions = new List<AttributeDefinition>()
            {
                new AttributeDefinition
                {
                    AttributeName = primaryKeyColumnName,
                    AttributeType = "S",
                },
                new AttributeDefinition
                {
                    AttributeName = sortKeyColumnName,
                    AttributeType = "S",
                }
            },
            KeySchema = new List<KeySchemaElement>()
            {
                new KeySchemaElement
                {
                    AttributeName = primaryKeyColumnName,
                    KeyType = "HASH",
                },
                new KeySchemaElement
                {
                    AttributeName = sortKeyColumnName,
                    KeyType = "RANGE",
                },
            },
            BillingMode = BillingMode.PAY_PER_REQUEST,
        });

        // Wait until the table is ACTIVE and then report success.
        Console.Write("Waiting for table to become active...");

        var request = new DescribeTableRequest
        {
            TableName = response.TableDescription.TableName,
        };

        TableStatus status;
        do
        {
            await Task.Delay(2000);

            var describeTableResponse = await client.DescribeTableAsync(request);
            status = describeTableResponse.Table.TableStatus;

            Console.Write(".");
        } while (status != "ACTIVE");

        return status == TableStatus.ACTIVE;
    }

    public static async Task<bool> TableExist(AmazonDynamoDBClient client, string tableName)
    {
        var request = new ListTablesRequest();
        //var request = new DescribeTableRequest(tableName);
        var response = await client.ListTablesAsync(request); //max 100 results

        return response?.HttpStatusCode == HttpStatusCode.OK && response.TableNames.Contains(tableName);
    }
}