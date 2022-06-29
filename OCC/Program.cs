using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using DynamoDbHelpers;

namespace OCC
{
    static class Program
    {
        const int numberOfConcurrentUpdates = 50;
        const int maxRetryAttempts = 50;
        const string sagasTableName = nameof(SagaDataWithOCC);

        static async Task Main(string[] args)
        {
            var clientConfig = new AmazonDynamoDBConfig()
            {
                RegionEndpoint = RegionEndpoint.EUSouth1,
            };
            var dynamoDbClient = new AmazonDynamoDBClient(new EnvironmentVariablesAWSCredentials(), clientConfig);

            var tableExist = await TableHelper.TableExist(dynamoDbClient, sagasTableName);
            if (tableExist == false)
            {
                _ = await TableHelper.CreateTable( dynamoDbClient, sagasTableName, nameof(SagaDataWithOCC.SagaID), nameof(SagaDataWithOCC.CorrelationID));
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
            var handledIndexes = ExtractHandledIndexes(getSagaResponse.Item);
            
            Console.WriteLine($"# of indexes stored in the document:\t{handledIndexes.Count}");
            Console.WriteLine($"# of reported successful updates:\t{succeededUpdates.Count}");
            Console.WriteLine();

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
            var handledIndexes = sagaData[nameof(SagaDataWithOCC.HandledIndexes)].L.Select(av => int.Parse(av.N)).ToList();
            return handledIndexes;
        }
        
        static int ExtractVersion(Dictionary<string, AttributeValue> sagaData)
        {
            var version = sagaData[nameof(SagaDataWithOCC.ItemVersion)].N;
            return int.Parse(version);
        }

        static Dictionary<string, AttributeValue> CreateKey(string sagaDataStableId)
        {
            return new Dictionary<string, AttributeValue>()
            {
                [nameof(SagaDataWithOCC.SagaID)] = new AttributeValue { S = sagaDataStableId },
                [nameof(SagaDataWithOCC.CorrelationID)] = new AttributeValue { S = sagaDataStableId },
            };
        }

        static async Task<bool> ValidateResults(AmazonDynamoDBClient client, string sagaDataStableId, (bool Succeeded, int Index, string ErrorMessage)[] results)
        {
            var getSagaRequest = new GetItemRequest(sagasTableName, CreateKey(sagaDataStableId))
            {
                ConsistentRead = true
            };
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
            item.Add(nameof(SagaDataWithOCC.ItemVersion), new AttributeValue()
            {
                N = "0"
            });
            item.Add(nameof(SagaDataWithOCC.HandledIndexes), new AttributeValue()
            {
                L = new List<AttributeValue>(),
                IsLSet = true
            });
            
            var request = new PutItemRequest
            {
                TableName = sagasTableName,
                Item = item
            };
            _ = await dynamoDbClient.PutItemAsync(request);
            
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
                    var getSagaDataRequest = new GetItemRequest(sagasTableName, CreateKey(sagaDataStableId))
                    {
                        ConsistentRead = true
                    };

                    var getItemResponse = await dynamoDbClient.GetItemAsync(getSagaDataRequest);

                    var currentIndexes = ExtractHandledIndexes(getItemResponse.Item);
                    var currentVersion = ExtractVersion(getItemResponse.Item);
                    var nextVersion = currentVersion + 1;

                    var updateSagaDataRequest = new UpdateItemRequest()
                    {
                        TableName = sagasTableName,
                        Key = CreateKey(sagaDataStableId),
                        UpdateExpression = $"SET {nameof(SagaDataWithOCC.HandledIndexes)}[{currentIndexes.Count + 1}] = :hi, {nameof(SagaDataWithOCC.ItemVersion)} = :nv",
                        ConditionExpression = $"{nameof(SagaDataWithOCC.ItemVersion)} = :cv",
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                        {
                            {
                                ":hi", new AttributeValue()
                                {
                                    N = index.ToString()
                                }
                            },
                            {
                                ":nv", new AttributeValue()
                                {
                                    N = nextVersion.ToString()
                                }
                            },
                            {
                                ":cv", new AttributeValue()
                                {
                                    N = currentVersion.ToString()
                                }
                            }
                        }
                    };

                    await dynamoDbClient.UpdateItemAsync(updateSagaDataRequest);

                    Console.WriteLine($"Index {index} updated successfully after {attempts} attempts.");

                    return (true, index, string.Empty);
                }
                catch (ConditionalCheckFailedException ccfex)
                {
                    Console.WriteLine($"Concurrency exception while handling index {index}, for saga {sagaDataStableId}");
                    attempts++;
                    lastError = ccfex;
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

    class SagaDataWithOCC
    {
        public string SagaID { get; set; }
        public string CorrelationID { get; set; }
        public int ItemVersion { get; set; }
        public List<int> HandledIndexes { get; set; } = new List<int>();
    }
}