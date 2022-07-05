using System.Net.Http.Headers;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using DynamoDbHelpers;

namespace BatchGetVsQuery;

public static class Program
{
    static void PrintTotalCapacity(List<ConsumedCapacity> consumedCapacity)
    {
        foreach (var cc in consumedCapacity)
        {
            Console.WriteLine("RCU: " + cc.ReadCapacityUnits);
            Console.WriteLine("WCU: " + cc.WriteCapacityUnits);
            Console.WriteLine("CU: " + cc.CapacityUnits);
        }
    }

    public static async Task Main()
    {
        var clientConfig = new AmazonDynamoDBConfig()
        {
            RegionEndpoint = RegionEndpoint.EUSouth1,
        };
        var dynamoDbClient = new AmazonDynamoDBClient(new EnvironmentVariablesAWSCredentials(), clientConfig);

        var tableName = "BatchGetVsQueryTests";
        // var tableExist = await TableHelper.TableExist(dynamoDbClient, tableName);
        // if (tableExist == false)
        // {
        //     _ = await TableHelper.CreateTable(dynamoDbClient, tableName, "PK", "SK");
        // }

        var pk = "some-partition-id";
        var items = Enumerable.Range(0, 10);

        // var request = new BatchWriteItemRequest()
        // {
        //     ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL
        // };
        //
        // var writeRequests = new List<WriteRequest>();
        // foreach (var item in items)
        // {
        //     writeRequests.Add(new WriteRequest()
        //     {
        //         PutRequest = new PutRequest()
        //         {
        //             Item = new Dictionary<string, AttributeValue>()
        //             {
        //                 { "PK", new AttributeValue(pk) },
        //                 { "SK", new AttributeValue($"{pk}#{item}") },
        //                 { "Val", new AttributeValue(new string('x', 1024 * 150)) }
        //             }
        //         }
        //     });
        // }
        //
        // request.RequestItems = new Dictionary<string, List<WriteRequest>>()
        // {
        //     { tableName, writeRequests }
        // };
        //
        // var response = await dynamoDbClient.BatchWriteItemAsync(request);
        // Console.WriteLine("First attempt");
        // PrintTotalCapacity(response.ConsumedCapacity);
        //
        // if (response.UnprocessedItems.Count > 0)
        // {
        //     var secondAttempt = await dynamoDbClient.BatchWriteItemAsync(new BatchWriteItemRequest() { RequestItems = response.UnprocessedItems });
        //     if (secondAttempt.UnprocessedItems.Count > 0)
        //     {
        //         throw new Exception("I gave up!");
        //     }
        //
        //     Console.WriteLine("Second attempt");
        //     PrintTotalCapacity(secondAttempt.ConsumedCapacity);
        // }


        // Console.WriteLine("Starting to query");
        //
        // QueryResponse queryResponse = null;
        // do
        // {
        //     var queryRequest = new QueryRequest
        //     {
        //         ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL,
        //         KeyConditionExpression = "PK = :incomingId",
        //         ExclusiveStartKey = queryResponse?.LastEvaluatedKey,
        //         ExpressionAttributeValues = new Dictionary<string, AttributeValue>
        //         {
        //             { ":incomingId", new AttributeValue { S = pk } }
        //         },
        //         TableName = tableName
        //     };
        //     queryResponse = await dynamoDbClient.QueryAsync(queryRequest).ConfigureAwait(false);
        //
        //     Console.WriteLine("Page: ");
        //     Console.WriteLine("RCU: " + queryResponse.ConsumedCapacity.ReadCapacityUnits);
        //     Console.WriteLine("WCU: " + queryResponse.ConsumedCapacity.WriteCapacityUnits);
        //     Console.WriteLine("CU: " + queryResponse.ConsumedCapacity.CapacityUnits);
        //
        //     foreach (var queryResponseItem in queryResponse.Items)
        //     {
        //         Console.WriteLine(queryResponseItem["SK"].S);
        //     }
        // } while (queryResponse.LastEvaluatedKey.Count > 0);

        
        Console.WriteLine("Batch read of 10");
        //batch get of 10
        var listOfItemsToRetrieve = new List<Dictionary<string, AttributeValue>>();
        foreach (var item in items)
        {
            listOfItemsToRetrieve.Add(new Dictionary<string, AttributeValue>()
            {
                { "PK", new AttributeValue() { S = pk } },
                { "SK", new AttributeValue() { S = $"{pk}#{item}" } }
            });
        }

        var batchRequestOf10 = new BatchGetItemRequest
        {
            ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL,
            RequestItems = new Dictionary<string, KeysAndAttributes>()
            {
                { tableName, new KeysAndAttributes() { Keys = listOfItemsToRetrieve } }
            }
        };
        var batchOf10ItemsResponse = await dynamoDbClient.BatchGetItemAsync(batchRequestOf10);
        if (batchOf10ItemsResponse.UnprocessedKeys.Count > 0)
        {
            Console.WriteLine($"Batch get of 10 has unprocessed keys: {batchOf10ItemsResponse.UnprocessedKeys.Count}.");
        }

        foreach (var returnedItems in batchOf10ItemsResponse.Responses.Single().Value)
        {
            Console.WriteLine(returnedItems["SK"].S);
        }
        
        PrintTotalCapacity(batchOf10ItemsResponse.ConsumedCapacity);
        
        Console.Read();
    }
}