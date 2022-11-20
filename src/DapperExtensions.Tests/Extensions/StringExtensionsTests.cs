using NUnit.Framework;
using DapperExtensions.Extensions;

namespace DapperExtensions.Tests.Extensions
{
    internal class StringExtensionsTests
    {
        private string? sqlInjectDrop;
        private string? sqlTempTableCreationFromVarchar;
        private string? sqlTempTableCreationFromInt;

        [SetUp]
        public void Setup()
        {
            sqlInjectDrop = "--DROP table";
            sqlTempTableCreationFromVarchar = "varchar(255)";
            sqlTempTableCreationFromInt = "int";
        }

        [Test]
        public void CanDetectSQLInjectionForTempTable()
        {
            Assert.IsTrue(sqlInjectDrop.ContainsSQLInjectionKeywords());
            Assert.IsFalse(sqlTempTableCreationFromInt.ContainsSQLInjectionKeywords());
            Assert.IsFalse(sqlTempTableCreationFromVarchar.ContainsSQLInjectionKeywords());
        }
    }
}