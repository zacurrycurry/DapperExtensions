using Dapper.Extensions;
using DapperExtensions;
using Microsoft.Data.SqlClient;
using Moq;
using NUnit.Framework;
using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace DapperExtensions.Tests.Extensions
{
    internal class SqlConnectionExtensionsTests
    {
        private readonly SqlException _sqlDeadlockException;
        private readonly SqlException _sqlGeneralNetworkException;
        private readonly SqlException _sqlTimeoutException;
        private readonly SqlException _encryptionNotSupportedException;

        public SqlConnectionExtensionsTests()
        {
            _sqlDeadlockException = SqlExceptionHelper.Generate(1205);
            _sqlGeneralNetworkException = SqlExceptionHelper.Generate(11);
            _sqlTimeoutException = SqlExceptionHelper.Generate(-2);
            _encryptionNotSupportedException = SqlExceptionHelper.Generate(20);
        }

        [Test]
        public async Task WillRetryATaskForSqlDeadlockException()
        {
            var invocations = 0;
            var policy = DapperConnectionExtensions.GetSqlExponentialBackoffPolicy(3);
            var result = await policy
                .ExecuteAsync(() => Task.FromResult(invocations++ < 1 ? throw _sqlDeadlockException : true));

            Assert.True(result);
            Assert.AreEqual(2, invocations);
        }

        [Test]
        public async Task WillRetryATaskForSqlDeadlockAndTimeoutAndNetworkException()
        {
            var invocations = 0;
            var policy = DapperConnectionExtensions.GetSqlExponentialBackoffPolicy(4);
            var result = await policy
                .ExecuteAsync(() =>
                {
                    invocations++;
                    if (invocations == 1)
                    {
                        throw _sqlDeadlockException;
                    }
                    else if (invocations == 2)
                    {
                        throw _sqlTimeoutException;
                    }
                    else if (invocations == 3)
                    {
                        throw _sqlGeneralNetworkException;
                    }
                    return Task.FromResult(true);
                });

            Assert.True(result);
            Assert.AreEqual(4, invocations);
        }

        [Test]
        public async Task WillExecuteAndNotRetryATaskThatDoesNotThrowAnException()
        {
            var invocations = 0;
            var policy = DapperConnectionExtensions.GetSqlExponentialBackoffPolicy(3);
            var result = await policy
                .ExecuteAsync(() => Task.FromResult(invocations++ < 1));

            Assert.True(result);
            Assert.AreEqual(1, invocations);
        }

        [Test]
        public void WillFailATaskForSqlEncryptionNotSupportedException()
        {
            var invocations = 0;
            var policy = DapperConnectionExtensions.GetSqlExponentialBackoffPolicy(3);
            var ex = Assert.ThrowsAsync<SqlException>(async () => await policy.ExecuteAsync(() =>
                Task.FromResult(invocations++ <= 1 ? throw _encryptionNotSupportedException : true)));
            if (ex != null)
            {
                Assert.That(ex.Number, Is.EqualTo(20));
            }
            Assert.AreEqual(1, invocations);
        }

        [Test]
        public async Task CanCheckAndReOpenBrokenConnection()
        {
            var connectionMock = new Mock<DbConnection>();
            connectionMock.SetupGet(x => x.State)
                .Returns(ConnectionState.Broken);

            var connection = connectionMock.Object;
            await connection.CheckAndReOpenConnection();
            connectionMock.Verify(x => x.Close(), Times.Once());
            connectionMock.Verify(x => x.OpenAsync(new CancellationToken()), Times.Once());
        }

        [Test]
        public async Task CanUseOpenConnection()
        {
            var connectionMock = new Mock<DbConnection>();
            var connection = connectionMock.Object;
            connectionMock.SetupGet(x => x.State)
                .Returns(ConnectionState.Open);

            await connection.CheckAndReOpenConnection();
            connectionMock.Verify(x => x.Close(), Times.Never());
            connectionMock.Verify(x => x.OpenAsync(new CancellationToken()), Times.Never());
        }

        [Test]
        public async Task CanOpenConnection()
        {
            var connectionMock = new Mock<DbConnection>();
            var connection = connectionMock.Object;
            await connection.CheckAndReOpenConnection();
            connectionMock.Verify(x => x.Close(), Times.Never());
            connectionMock.Verify(x => x.OpenAsync(new CancellationToken()), Times.Once());
        }

        [Test]
        public void ThrowsArgumentExceptionForNullConnection()
        {
            Assert.ThrowsAsync<ArgumentNullException>(async () =>
                await DapperConnectionExtensions.CheckAndReOpenConnection(null)
            );
        }
    }
}