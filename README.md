# DapperExtensions.Z
A useful set of extension methods for Dapper

## BulkUpload Service
Leverages reflection to map to the target table and SqlBulkCopy for ultra-fast bulk inserts

> BulkUploadService.BulkUploadAsync<T>(string tableSchema, string tableName, string connectionString, IEnumerable<T> entities, int batchSize = 1000, int numberOfRetries = 5)

## SqlConnection Retry Extension Methods
Dapper methods wrapped with exponential retry logic to handle SQL timeouts, general network errors, and deadlocks

> ExecuteWithRetryAsync
>
> ExecuteScalarWithRetryAsync
>
> QueryFirstWithRetryAsync
>
> QueryFirstOrDefaultWithRetryAsync
>
> QuerySingleWithRetryAsync
>
> QuerySingleOrDefaultWithRetryAsync
>
> QueryWithRetryAsync
>
> QueryWithTempTableAndRetryAsync

