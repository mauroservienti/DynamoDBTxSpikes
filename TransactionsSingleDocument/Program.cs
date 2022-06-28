using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace TransactionsSingleDocument
{
    static class Program
    {
        const int numberOfConcurrentUpdates = 50;
        const int maxRetryAttempts = 50;
        const string sagasTableName = "Sagas";

        static async Task<bool> CreateTable(AmazonDynamoDBClient client, string tableName)
        {
            var response = await client.CreateTableAsync(new CreateTableRequest
            {
                TableName = tableName,
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition
                    {
                        AttributeName = "SagaID",
                        AttributeType = "S",
                    },
                    new AttributeDefinition
                    {
                        AttributeName = "CorrelationID",
                        AttributeType = "S",
                    }
                },
                KeySchema = new List<KeySchemaElement>()
                {
                    new KeySchemaElement
                    {
                        AttributeName = "SagaID",
                        KeyType = "HASH",
                    },
                    new KeySchemaElement
                    {
                        AttributeName = "CorrelationID",
                        KeyType = "RANGE",
                    },
                },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    //no idea what these values should be in production and it users should have control on it and how
                    ReadCapacityUnits = 50,
                    WriteCapacityUnits = 50,
                },
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
            }
            while (status != "ACTIVE");

            return status == TableStatus.ACTIVE;
        }

        static async Task<bool> TableExist(AmazonDynamoDBClient client, string tableName)
        {
            var request = new ListTablesRequest();
            var response = await client.ListTablesAsync(request); //max 100 results

            return response?.HttpStatusCode == HttpStatusCode.OK && response.TableNames.Contains(tableName);
        }

        static async Task Main(string[] args)
        {
            var clientConfig = new AmazonDynamoDBConfig()
            {
                RegionEndpoint = RegionEndpoint.EUSouth1,
            };
            var dynamoDbClient = new AmazonDynamoDBClient(clientConfig);

            var tableExist = await TableExist(dynamoDbClient, sagasTableName);
            if (tableExist == false)
            {
                _ = await CreateTable( dynamoDbClient, sagasTableName);
            }

            (bool Succeeded, int Index, string ErrorMessage)[] results = null;
            var succeeded = true;
            string sagaDataStableId = null;
            
            while (succeeded)
            {
                sagaDataStableId = Guid.NewGuid().ToString();
                results = await Execute(dynamoDbClient, sagaDataStableId);
            
                succeeded = await ValidateResults(dynamoDbClient, sagaDataStableId, results);
                if (succeeded)
                {
                    Console.WriteLine();
                    Console.WriteLine("Execution completed, going to try again...");
            
                }
            }
            
            Console.WriteLine();
            Console.WriteLine("-----------------------------------------------------------------------------------------------------");
            Console.WriteLine($"Execution completed with errors for document '{sagaDataStableId}'");
            Console.WriteLine("-----------------------------------------------------------------------------------------------------");
            Console.WriteLine();
            
            var succeededUpdates = results.Where(r => r.Succeeded).ToList();
            var failedUpdates = results.Where(r => !r.Succeeded).ToList();
            
            if (failedUpdates.Any())
            {
                Console.WriteLine($"{failedUpdates.Count} updated failed.");
                foreach (var failure in failedUpdates)
                {
                    Console.WriteLine($"\t{failure.ErrorMessage}");
                }
            }

            var getSagaRequest = new GetItemRequest(sagasTableName, CreateKey(sagaDataStableId))
            {
                ConsistentRead = true
            };
            var getSagaResponse = await dynamoDbClient.GetItemAsync(getSagaRequest);
            var sagaData = getSagaResponse.Item;
            
            Console.WriteLine($"# of indexes stored in the document:\t{sagaData[nameof(SampleSagaData.HandledIndexes)].NS.Count}");
            Console.WriteLine($"# of reported successful updates:\t{succeededUpdates.Count}");
            Console.WriteLine();

            var handledIndexes = ExtractHandledIndexes(sagaData);
            var diff = Enumerable.Range(0, numberOfConcurrentUpdates - 1)
                .Except(failedUpdates.Select(fu => fu.Index))
                .Except(handledIndexes)
                .ToList();
            
            if (diff.Any())
            {
                Console.WriteLine("Cannot find an update for the following index(es): ");
                foreach (var idx in diff)
                {
                    Console.WriteLine($"\t{idx}");
                }
            }
            else
            {
                Console.WriteLine("Found all expected indexes.");
            }
        }

        static List<int> ExtractHandledIndexes(Dictionary<string, AttributeValue> sagaData)
        {
            var handledIndexes = sagaData[nameof(SampleSagaData.HandledIndexes)].NS.Select(stringValue => int.Parse(stringValue)).ToList();
            return handledIndexes;
        }

        static Dictionary<string, AttributeValue> CreateKey(string sagaDataStableId)
        {
            return new Dictionary<string, AttributeValue>()
            {
                ["SagaID"] = new AttributeValue { S = sagaDataStableId },
                ["CorrelationID"] = new AttributeValue { S = sagaDataStableId },
            };
        }

        static async Task<bool> ValidateResults(AmazonDynamoDBClient client, string sagaDataStableId, (bool Succeeded, int Index, string ErrorMessage)[] results)
        {
            var getSagaRequest = new GetItemRequest(sagasTableName, CreateKey(sagaDataStableId));
            var getSagaResponse = await client.GetItemAsync(getSagaRequest);
            var sagaData = getSagaResponse.Item;
            var handledIndexes = ExtractHandledIndexes(sagaData);
        
            var updatesFailedDueToConcurrency = results.Where(r => !r.Succeeded).ToList();
        
            var diff = Enumerable.Range(0, numberOfConcurrentUpdates - 1)
                .Except(updatesFailedDueToConcurrency.Select(fu => fu.Index))
                .Except(handledIndexes)
                .ToList();
        
            return diff.Count == 0;
        }
        
        static async Task<(bool, int, string)[]> Execute(AmazonDynamoDBClient dynamoDbClient, string sagaDataStableId)
        {
            Console.WriteLine($"Test document ID: {sagaDataStableId}");

            var item = CreateKey(sagaDataStableId);
            item.Add("HandledIndexes", new AttributeValue()
            {
                NS = new List<string>(),
                IsLSet = true
            });
            
            var request = new PutItemRequest
            {
                TableName = sagasTableName,
                Item = item
            };
            var response = await dynamoDbClient.PutItemAsync(request);
            
            Console.WriteLine($"Test document created. Ready to try to concurrently update document {numberOfConcurrentUpdates} times.");
            Console.WriteLine();
        
            var pendingTasks = new List<Task<(bool Succeeded, int Index, string ErrorMessage)>>();
            for (var i = 0; i < numberOfConcurrentUpdates; i++)
            {
                pendingTasks.Add(TouchSaga(i, dynamoDbClient, sagaDataStableId));
            }
        
            var results = await Task.WhenAll(pendingTasks);
        
            return results;
        }
        
        static async Task<(bool, int, string)> TouchSaga(int index, AmazonDynamoDBClient dynamoDbClient, string sagaDataStableId)
        {
            var attempts = 0;
            Exception lastError = null;
        
            while (attempts <= maxRetryAttempts)
            {
                try
                {
                    var getSagaDataTxRequest = new TransactGetItemsRequest()
                    {
                        TransactItems = new List<TransactGetItem>()
                        {
                            new()
                            {
                                Get = new Get()
                                {
                                    Key = CreateKey(sagaDataStableId),
                                    TableName = sagasTableName
                                }
                            }
                        }
                    };

                    var getSagaDataTxItemResponse = await dynamoDbClient.TransactGetItemsAsync(getSagaDataTxRequest);
                    var itemResponse = getSagaDataTxItemResponse.Responses[0];
                    
                    var currentIndexes = ExtractHandledIndexes(itemResponse.Item);

                    var updateSagaDataTxRequest = new TransactWriteItemsRequest()
                    {
                        TransactItems = new List<TransactWriteItem>()
                        {
                            new ()
                            { 
                                Update = new Update()
                                {
                                    Key = CreateKey(sagaDataStableId),
                                    TableName = sagasTableName,
                                    UpdateExpression = $"SET HandledIndexes[{currentIndexes.Count + 1}] = :hi",
                                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                                    {
                                        {
                                            ":hi", new AttributeValue()
                                            {
                                                N = index.ToString()
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    };
                    
                    await dynamoDbClient.TransactWriteItemsAsync(updateSagaDataTxRequest);
        
                    Console.WriteLine($"Index {index} updated successfully after {attempts} attempts.");
        
                    return (true, index, string.Empty);
                }
                catch (Exception ex)
                {
                    attempts++;
                    lastError = ex;
                }
            }
        
            Console.WriteLine($"Failed after {attempts} attempts, while handling index {index}, last error: {lastError?.Message}");
        
            return (false, index, $"Failed after {attempts} attempts, while handling index {index}, last error: {lastError?.Message}");
        }
    }

    class SampleSagaData
    {
        public string SagaId { get; set; }
        public string CorrelationId { get; set; }
        public List<int> HandledIndexes { get; set; } = new List<int>();
    }
}