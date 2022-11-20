using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace DapperExtensions.Tests
{
    internal sealed class SqlExceptionHelper
    {
        internal static SqlException Generate(int errorNumber)
        {
            var ex = (SqlException)FormatterServices.GetUninitializedObject(typeof(SqlException));
            var errors = GenerateSqlErrorCollection(errorNumber);
            SetPrivateFieldValue(ex, "_errors", errors);
            return ex;
        }

        internal static SqlErrorCollection GenerateSqlErrorCollection(int errorNumber)
        {
            var t = typeof(SqlErrorCollection);
            var col = (SqlErrorCollection)FormatterServices.GetUninitializedObject(t);
            SetPrivateFieldValue(col, "_errors", new List<object>());
            var sqlError = GenerateSqlError(errorNumber);
            var method = t.GetMethod("Add", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            if (method != null)
            {
                method.Invoke(col, new object[] { sqlError });
                return col;
            }
            throw new Exception("Could not get method 'Add'");
        }

        private static SqlError GenerateSqlError(int errorNumber)
        {
            var sqlError = (SqlError)FormatterServices.GetUninitializedObject(typeof(SqlError));
            SetPrivateFieldValue(sqlError, "_number", errorNumber);
            SetPrivateFieldValue(sqlError, "_message", string.Empty);
            SetPrivateFieldValue(sqlError, "_procedure", string.Empty);
            SetPrivateFieldValue(sqlError, "_server", string.Empty);
            SetPrivateFieldValue(sqlError, "_source", string.Empty);
            return sqlError;
        }

        private static void SetPrivateFieldValue(object obj, string field, object val)
        {
            var member = obj.GetType().GetField(field, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            if (member != null)
            {
                member.SetValue(obj, val);
            }
            else
            {
                throw new Exception($"Could not set private field {field} on object: {obj.ToString()}");
            }
        }
    }
}