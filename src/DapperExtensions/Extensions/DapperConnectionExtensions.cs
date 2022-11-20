using Ardalis.GuardClauses;
using DapperExtensions.Extensions;
using FastMember;
using Microsoft.Data.SqlClient;
using Polly;
using Polly.Retry;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.Extensions
{
    public static class DapperConnectionExtensions
    {
        //https://docs.microsoft.com/en-us/sql/relational-databases/errors-events/database-engine-events-and-errors?view=sql-server-ver15
        private static readonly HashSet<int> _sqlErrorNumbersToRetry = new HashSet<int>(new int[]
        {
            -2, // timeout
            11, // general network error
            1205 // deadlock
        });

        /// <summary>
        /// Execute parameterized SQL asyncronously using a task with an with an exponential backoff retry policy that selects a single value.
        /// </summary>
        /// <typeparam name="T">The type to return.</typeparam>
        /// <param name="connection">The connection to query on.</param>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="parameters">The parameters to pass, if any.</param>
        /// <param name="transaction">The transaction to use, if any.</param>
        /// <param name="commandTimeout">The number of seconds before command execution timeout.</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="numberOfRetries">The maximum number of attempts to retry.</param>
        /// <returns>
        /// The first cell returned, as T.
        /// </returns>
        public static async Task<T> ExecuteScalarWithRetryAsync<T>(this SqlConnection connection, string sql, object parameters = null,
            IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null, int numberOfRetries = 5)
        {
            Guard.Against.Null(connection, nameof(connection));
            Guard.Against.NullOrWhiteSpace(sql, nameof(sql));

            await connection.CheckAndReOpenConnection();
            return await connection.ExecuteScalarAsync<T>(sql, parameters, transaction, commandTimeout, commandType)
                .WithRetry(numberOfRetries);
        }

        /// <summary>
        /// Executes a command asyncronously using a task with an with an exponential backoff retry policy
        /// </summary>
        /// <typeparam name="T">The type of results to return.</typeparam>
        /// <param name="connection">The connection to query on.</param>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="parameters">The parameters to pass, if any.</param>
        /// <param name="transaction">The transaction to use, if any.</param>
        /// <param name="commandTimeout">The number of seconds before command execution timeout.</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="numberOfRetries">The maximum number of attempts to retry.</param>
        /// <returns>
        /// The number of rows affected.
        /// </returns>
        public static async Task<int> ExecuteWithRetryAsync(this SqlConnection connection, string sql, object parameters = null,
            IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null, int numberOfRetries = 5)
        {
            Guard.Against.Null(connection, nameof(connection));
            Guard.Against.NullOrWhiteSpace(sql, nameof(sql));

            await connection.CheckAndReOpenConnection();
            return await connection.ExecuteAsync(sql, parameters, transaction, commandTimeout, commandType)
                .WithRetry(numberOfRetries);
        }

        /// <summary>
        /// Executes a query asyncronously using a task with an with an exponential backoff retry policy
        /// </summary>
        /// <typeparam name="T">The type of results to return.</typeparam>
        /// <param name="connection">The connection to query on.</param>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="parameters">The parameters to pass, if any.</param>
        /// <param name="transaction">The transaction to use, if any.</param>
        /// <param name="commandTimeout">The number of seconds before command execution timeout.</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="numberOfRetries">The maximum number of attempts to retry.</param>
        /// <returns>
        /// A sequence of data of T; if a basic type (int, string, etc) is queried then the
        /// data from the first column in assumed, otherwise an instance is created per row,
        /// and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public static async Task<IEnumerable<T>> QueryWithRetryAsync<T>(this SqlConnection connection, string sql, object parameters = null,
            IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null, int numberOfRetries = 5)
        {
            Guard.Against.Null(connection, nameof(connection));
            Guard.Against.NullOrWhiteSpace(sql, nameof(sql));

            await connection.CheckAndReOpenConnection();
            return await connection.QueryAsync<T>(sql, parameters, transaction, commandTimeout, commandType)
                .WithRetry(numberOfRetries);
        }

        /// <summary>
        /// Execute a single-row query asynchronously using Task with an with an exponential backoff retry policy
        /// </summary>
        /// <typeparam name="T">The type of results to return.</typeparam>
        /// <param name="connection">The connection to query on.</param>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="parameters">The parameters to pass, if any.</param>
        /// <param name="transaction">The transaction to use, if any.</param>
        /// <param name="commandTimeout">The number of seconds before command execution timeout.</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="numberOfRetries">The maximum number of attempts to retry.</param>
        /// <returns>
        /// A single instance of data of T; if a basic type (int, string, etc) is queried then the
        /// data from the first column in assumed, otherwise an instance is created per row,
        /// and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public static async Task<T> QuerySingleWithRetryAsync<T>(this SqlConnection connection, string sql, object parameters = null,
            IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null, int numberOfRetries = 5)
        {
            Guard.Against.Null(connection, nameof(connection));
            Guard.Against.NullOrWhiteSpace(sql, nameof(sql));

            await connection.CheckAndReOpenConnection();
            return await connection.QuerySingleAsync<T>(sql, parameters, transaction, commandTimeout, commandType)
                .WithRetry(numberOfRetries);
        }

        /// <summary>
        /// Execute a single-row query asynchronously using Task with an with an exponential backoff retry policy
        /// </summary>
        /// <typeparam name="T">The type of results to return.</typeparam>
        /// <param name="connection">The connection to query on.</param>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="parameters">The parameters to pass, if any.</param>
        /// <param name="transaction">The transaction to use, if any.</param>
        /// <param name="commandTimeout">The number of seconds before command execution timeout.</param>
        /// <param name="commandType">The type of command to execute.</param>
        /// <param name="numberOfRetries">The maximum number of attempts to retry.</param>
        /// <returns>
        /// A single instance of data of T; if a basic type (int, string, etc) is queried then the
        /// data from the first column in assumed, otherwise an instance is created per row,
        /// and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public static async Task<T> QuerySingleOrDefaultWithRetryAsync<T>(this SqlConnection connection, string sql, object parameters = null,
            IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null, int numberOfRetries = 5)
        {
            Guard.Against.Null(connection, nameof(connection));
            Guard.Against.NullOrWhiteSpace(sql, nameof(sql));

            await connection.CheckAndReOpenConnection();
            return await connection.QuerySingleOrDefaultAsync<T>(sql, parameters, transaction, commandTimeout, commandType)
                .WithRetry(numberOfRetries);
        }

        /// <summary>
        /// Creates a temp table named '#Temp' loaded with the items in the tempData parameter.
        /// The field to join on will be #Temp.Item which will be created as a type defined in the tempDataType parameter.
        /// A sample for using an int would be QueryWithRetryAndTempTableAsync(conn, querySQL, "int", listOfInts, querySQLParameters)
        /// A sample for using an string of length 10 would be QueryWithRetryAndTempTableAsync(conn, querySQL, "varchar(10)", listOfKeys, querySQLParameters)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="sql"></param>
        /// <param name="tempDataType">specif</param>
        /// <param name="tempData"></param>
        /// <param name="parameters"></param>
        /// <param name="batchSize"></param>
        /// <param name="numberOfRetries"></param>
        /// <returns>
        /// A sequence of data of T; if a basic type (int, string, etc) is queried then the
        /// data from the first column in assumed, otherwise an instance is created per row,
        /// and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        public static async Task<IEnumerable<T>> QueryWithTempTableAndRetryAsync<T>(this SqlConnection connection, string sql, string tempDataType,
            IEnumerable<string> tempData, object parameters = null, int batchSize = 10000, int numberOfRetries = 5)
        {
            Guard.Against.Null(tempData, nameof(tempData));
            Guard.Against.NullOrWhiteSpace(tempDataType, nameof(tempDataType));

            if (tempDataType.ContainsSQLInjectionKeywords())
            {
                throw new ArgumentException($"Attempted to inject SQL into temp table query: {tempDataType}", nameof(tempDataType));
            }

            var tmp = "#Temp";
            await connection.ExecuteWithRetryAsync($"CREATE TABLE #Temp (Item {tempDataType} not null)");

            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                using (var reader = ObjectReader.Create(tempData.Distinct(StringComparer.InvariantCultureIgnoreCase).Select(x => new { Item = x })))
                {
                    bulkCopy.ColumnMappings.Add("Item", 0);
                    bulkCopy.BatchSize = batchSize;
                    bulkCopy.DestinationTableName = tmp;
                    await bulkCopy.WriteToServerAsync(reader);
                    return await connection.QueryWithRetryAsync<T>(sql, parameters)
                        .WithRetry(numberOfRetries);
                }
            }
        }

        /// <summary>
        /// Check to see see if the connection is in the 'Open' ConnectionState
        /// If the connection is broken, it will close the connection safely.
        /// Then if the ConnectionState is anything other than 'Open', it will attempt to re-open the connection
        /// </summary>
        /// <param name="connection"></param>
        public static async Task CheckAndReOpenConnection(this DbConnection connection)
        {
            Guard.Against.Null(connection, nameof(connection));

            if (connection.State.HasFlag(ConnectionState.Broken))
            {
                connection.Close();
            }

            if (!connection.State.HasFlag(ConnectionState.Open))
            {
                await connection.OpenAsync();
            }
        }

        /// <summary>
        /// https://github.com/App-vNext/Polly
        /// Retry a task a specified number of times with an exponential backoff retry policy
        /// catching general SQL exceptions such as deadlocks, timeouts, and network errors
        /// The default retry attempts of 5 will wait for
        ///  2 ^ 1 = 2 seconds then
        ///  2 ^ 2 = 4 seconds then
        ///  2 ^ 3 = 8 seconds then
        ///  2 ^ 4 = 16 seconds then
        ///  2 ^ 5 = 32 seconds
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="task">The task to retry</param>
        /// <param name="retryAttempts">The maximum number of attempts to retry</param>
        internal static async Task<T> WithRetry<T>(this Task<T> task, int retryAttempts = 5) =>
            await GetSqlExponentialBackoffPolicy(retryAttempts)
                .ExecuteAsync(async () => await task);

        /// <summary>
        /// https://github.com/App-vNext/Polly
        /// Retry a task a specified number of times with an exponential backoff retry policy
        /// catching general SQL exceptions such as deadlocks, timeouts, and network errors
        /// The default retry attempts of 5 will wait for
        ///  2 ^ 1 = 2 seconds then
        ///  2 ^ 2 = 4 seconds then
        ///  2 ^ 3 = 8 seconds then
        ///  2 ^ 4 = 16 seconds then
        ///  2 ^ 5 = 32 seconds
        /// </summary>
        /// <param name="task">The task to retry</param>
        /// <param name="retryAttempts">The maximum number of attempts to retry</param>
        internal static async Task WithRetry(this Task task, int retryAttempts = 5) =>
            await GetSqlExponentialBackoffPolicy(retryAttempts)
                .ExecuteAsync(async () => await task);

        internal static AsyncRetryPolicy GetSqlExponentialBackoffPolicy(int retryAttempts = 5) =>
            Policy
                .Handle<SqlException>(ex => _sqlErrorNumbersToRetry.Contains(ex.Number))
                .WaitAndRetryAsync(
                    retryAttempts,
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    (exception, timeSpan, retryCount, context) =>
                    {
                        if (exception is SqlException sqlException)
                        {
                            Console.WriteLine($"Exception ({sqlException.Number}): {exception.Message}");
                            Console.WriteLine($"Retrying in {timeSpan.Seconds} seconds");
                        }
                    });
    }
}