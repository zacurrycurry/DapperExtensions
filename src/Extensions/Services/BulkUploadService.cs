﻿using Dapper.Extensions;
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
        public static async Task BulkUploadAsync<T>(string tableSchema, string tableName, string connectionString, IEnumerable<T> data, int batchSize = 1000, int numberOfRetries = 5)
        {
            var columns = new Dictionary<int, string>();
            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                var propNames = new HashSet<string>(typeof(T).GetProperties().Select(x => x.Name).ToList());
                var schema = await connection.GetTableSchemaAsync(tableName, tableSchema)
                    .WithRetry(numberOfRetries);
                var allDbFields = schema.Select(x => x.COLUMN_NAME).ToList();

                using (var bulkCopy = new SqlBulkCopy(connectionString, SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.CheckConstraints))
                {
                    bulkCopy.BulkCopyTimeout = 30;
                    bulkCopy.BatchSize = batchSize;
                    bulkCopy.DestinationTableName = $"[{tableSchema}].[{tableName}]";

                    for (var i = 0; i < allDbFields.Count; i++)
                    {
                        columns.Add(i, allDbFields[i]);
                        bulkCopy.ColumnMappings.Add(allDbFields[i], i);
                    }

                    var datatable = ToDataTable(data, schema);
                    await bulkCopy.WriteToServerAsync(datatable)
                        .WithRetry(numberOfRetries);
                }
            }
        }

        private static async Task<IEnumerable<SchemaInfo>> GetTableSchemaAsync(this SqlConnection connection, string tableName, string tableSchema = "dbo")
        {
            return await connection.QueryWithRetryAsync<SchemaInfo>(QueryHelper.TableSchema.Select, new { TableName = tableName, TableSchema = tableSchema });
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