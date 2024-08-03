using Dapper.Extensions;
using DapperExtensions.Extensions;
using DapperExtensions.Helpers;
using DapperExtensions.Models;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace DapperExtensions.Services
{
    public static class BulkUploadService
    {
        /// <summary>
        /// Leverages reflection to map to the target table and SqlBulkCopy for ultra-fast bulk inserts
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tableSchema">Table schema (i.e. dbo), if null will not use any schema - use for temp tables</param>
        /// <param name="tableName">Table name</param>
        /// <param name="connectionString">SQL connection string</param>
        /// <param name="entities">Data to upload</param>
        /// <param name="batchSize">Load data in batches of x</param>
        /// <param name="numberOfRetries">The maximum number of attempts to retry.</param>
        /// <param name="bulkCopyTimeout">The integer value of the Microsoft.Data.SqlClient.SqlBulkCopy.BulkCopyTimeout property. The default is 30 seconds. A value of 0 indicates no limit; the bulk copy will wait indefinitely.</param>
        /// <returns></returns>
        public static async Task BulkUploadAsync<T>(string tableSchema, string tableName, string connectionString, IEnumerable<T> entities, int batchSize = 1000, int numberOfRetries = 5, int bulkCopyTimeout = 30, SqlBulkCopyOptions sqlBulkCopyOptions = SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.TableLock)
        {
            var columns = new Dictionary<int, string>();
            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                var propNames = new HashSet<string>(typeof(T).GetProperties().Select(x => x.Name).ToList());
                var schema = await connection.GetTableSchemaAsync(tableName, tableSchema)
                    .WithRetry(numberOfRetries);
                var allDbFields = schema.Select(x => x.COLUMN_NAME).ToList();

                using (var bulkCopy = new SqlBulkCopy(connectionString, sqlBulkCopyOptions))
                {
                    bulkCopy.BulkCopyTimeout = bulkCopyTimeout;
                    bulkCopy.BatchSize = batchSize;
                    if (string.IsNullOrWhiteSpace(tableSchema))
                    {
                        bulkCopy.DestinationTableName = tableName;
                    }
                    else
                    {
                        bulkCopy.DestinationTableName = $"[{tableSchema}].[{tableName}]";
                    }

                    for (var i = 0; i < allDbFields.Count; i++)
                    {
                        columns.Add(i, allDbFields[i]);
                        bulkCopy.ColumnMappings.Add(allDbFields[i], i);
                    }

                    var datatable = ToDataTable(entities, schema);
                    await bulkCopy.WriteToServerAsync(datatable)
                        .WithRetry(numberOfRetries);
                }
            }
        }

        /// <summary>
        /// Leverages reflection to map to the target table and SqlBulkCopy for ultra-fast bulk inserts
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tableSchema">Table schema (i.e. dbo), if null will not use any schema - use for temp tables</param>
        /// <param name="tableName">Table name</param>
        /// <param name="connection">SQL connection</param>
        /// <param name="entities">Data to upload</param>
        /// <param name="batchSize">Load data in batches of x</param>
        /// <param name="numberOfRetries">The maximum number of attempts to retry.</param>
        /// <param name="bulkCopyTimeout">The integer value of the Microsoft.Data.SqlClient.SqlBulkCopy.BulkCopyTimeout property. The default is 30 seconds. A value of 0 indicates no limit; the bulk copy will wait indefinitely.</param>
        /// <returns></returns>
        public static async Task BulkUploadAsync<T>(string tableSchema, string tableName, SqlConnection connection, IEnumerable<T> entities, int batchSize = 1000, int numberOfRetries = 5, SqlTransaction transaction = null, int bulkCopyTimeout = 30, SqlBulkCopyOptions sqlBulkCopyOptions = SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.TableLock)
        {
            var columns = new Dictionary<int, string>();
            var propNames = new HashSet<string>(typeof(T).GetProperties().Select(x => x.Name).ToList());
            var schema = await connection.GetTableSchemaAsync(tableName, tableSchema, transaction)
                .WithRetry(numberOfRetries);
            var allDbFields = schema.Select(x => x.COLUMN_NAME).ToList();

            using (var bulkCopy = new SqlBulkCopy(connection, sqlBulkCopyOptions, externalTransaction: transaction))
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout;
                bulkCopy.BatchSize = batchSize;
                if (string.IsNullOrWhiteSpace(tableSchema))
                {
                    bulkCopy.DestinationTableName = tableName;
                }
                else
                {
                    bulkCopy.DestinationTableName = $"[{tableSchema}].[{tableName}]";
                }

                for (var i = 0; i < allDbFields.Count; i++)
                {
                    columns.Add(i, allDbFields[i]);
                    bulkCopy.ColumnMappings.Add(allDbFields[i], i);
                }

                var datatable = ToDataTable(entities, schema);
                await bulkCopy.WriteToServerAsync(datatable)
                    .WithRetry(numberOfRetries);
            }
        }

        /// <summary>
        /// Leverages reflection to map to the target table and SqlBulkCopy for ultra-fast bulk inserts
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tableSchema">Table schema (i.e. dbo), if null will not use any schema - use for temp tables</param>
        /// <param name="tableName">Table name</param>
        /// <param name="connection">SQL connection</param>
        /// <param name="entities">Data to upload</param>
        /// <param name="sqlBulkCopyOptions">SQL bulk copy options</param>
        /// <param name="batchSize">Load data in batches of x</param>
        /// <param name="numberOfRetries">The maximum number of attempts to retry.</param>
        /// <param name="bulkCopyTimeout">The integer value of the Microsoft.Data.SqlClient.SqlBulkCopy.BulkCopyTimeout property. The default is 30 seconds. A value of 0 indicates no limit; the bulk copy will wait indefinitely.</param>
        /// <returns></returns>
        public static async Task BulkUploadAsync<T>(string tableSchema, string tableName, SqlConnection connection, IEnumerable<T> entities, SqlBulkCopyOptions sqlBulkCopyOptions, int batchSize = 1000, int numberOfRetries = 5, SqlTransaction transaction = null, int bulkCopyTimeout = 30)
        {
            var columns = new Dictionary<int, string>();
            var propNames = new HashSet<string>(typeof(T).GetProperties().Select(x => x.Name).ToList());
            var schema = await connection.GetTableSchemaAsync(tableName, tableSchema, transaction)
                .WithRetry(numberOfRetries);
            var allDbFields = schema.Select(x => x.COLUMN_NAME).ToList();

            using (var bulkCopy = new SqlBulkCopy(connection, sqlBulkCopyOptions, externalTransaction: transaction))
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout;
                bulkCopy.BatchSize = batchSize;
                if (string.IsNullOrWhiteSpace(tableSchema))
                {
                    bulkCopy.DestinationTableName = tableName;
                }
                else
                {
                    bulkCopy.DestinationTableName = $"[{tableSchema}].[{tableName}]";
                }

                for (var i = 0; i < allDbFields.Count; i++)
                {
                    columns.Add(i, allDbFields[i]);
                    bulkCopy.ColumnMappings.Add(allDbFields[i], i);
                }

                var datatable = ToDataTable(entities, schema);
                await bulkCopy.WriteToServerAsync(datatable)
                    .WithRetry(numberOfRetries);
            }
        }

        private static async Task<IEnumerable<SchemaInfo>> GetTableSchemaAsync(this SqlConnection connection, string tableName, string tableSchema = "dbo", IDbTransaction transaction = null)
        {
            return await connection.QueryWithRetryAsync<SchemaInfo>(QueryHelper.TableSchema.Select, new { TableName = tableName, TableSchema = tableSchema }, transaction: transaction);
        }

        private static DataTable ToDataTable<T>(this IEnumerable<T> data, IEnumerable<SchemaInfo> cols)
        {
            var dataColumnNames = new HashSet<string>(typeof(T).GetProperties().Select(x => x.Name).ToList());
            var nullableColumns = new HashSet<string>(cols.Where(x => x.IS_NULLABLE).Select(x => x.COLUMN_NAME));
            var defaultConstraint = new HashSet<string>(cols.Where(x => !string.IsNullOrEmpty(x.COLUMN_DEFAULT)).Select(x => x.COLUMN_NAME));
            var table = new DataTable();
            foreach (var column in cols.OrderBy(x => x.ORDINAL_POSITION))
            {
                table.Columns.Add(column.COLUMN_NAME, column.GetDataType());
            }
            foreach (T item in data)
            {
                DataRow row = table.NewRow();
                foreach (var col in cols)
                {
                    var type = col.GetDataType();
                    var colName = col.COLUMN_NAME;
                    if (dataColumnNames.Contains(colName))
                    {
                        var value = item?.GetPropValue(colName);
                        if (value != null)
                        {
                            row[colName] = Convert.ChangeType(value, type);
                            continue;
                        }
                    }
                    if (nullableColumns.Contains(colName))
                    {
                        row[colName] = DBNull.Value;
                    }
                    else
                    {
                        if (type == typeof(DateTime))
                        {
                            row[colName] = DateTime.UtcNow;
                        }
                        else
                        {
                            row[colName] = type.GetDefault();
                        }
                    }
                }
                table.Rows.Add(row);
            }
            return table;
        }
    }
}