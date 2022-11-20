using DapperExtensions.Extensions;
using System.Reflection;

namespace DapperExtensions.Helpers
{
    internal class QueryHelper
    {
        private static readonly Assembly _assembly = Assembly.GetExecutingAssembly();

        internal static class TableSchema
        {
            internal static readonly string Select = _assembly.GetQuery("DapperExtensions.Queries.TableSchema.sql");
        }
    }
}