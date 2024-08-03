using DapperExtensions.Helpers;
using NUnit.Framework;

namespace DapperExtensions.Tests.Extensions
{
    internal class QueryHelperTests
    {
        [Test]
        public void CanGetAllQueries()
        {
            Assert.IsNotNull(QueryHelper.TableSchema.Select);
            Assert.IsNotNull(QueryHelper.TempTableSchema.Select);
        }
    }
}