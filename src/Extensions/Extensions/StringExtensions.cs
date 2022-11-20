using System;
using System.Collections.Generic;
using System.Linq;

namespace DapperExtensions.Extensions
{
    internal static class StringExtensions
    {
        internal static readonly string[] _injectionKeyWords = new string[]
        {
            "--", ";--", ";", "/*", "*/", "@@", "@", "alter", "begin", "cast", "create", "cursor",
            "declare", "delete", "drop", "end", "exec", "execute", "fetch", "insert", "kill",
            "select", "sys", "sysobjects", "syscolumns", "table", "update"
        };

        internal static bool ContainsAny(this string input, IEnumerable<string> containsKeywords, StringComparison comparisonType = StringComparison.OrdinalIgnoreCase)
        {
            return containsKeywords.Any(keyword => input.Contains(keyword));
        }

        internal static bool ContainsSQLInjectionKeywords(this string input)
        {
            if (string.IsNullOrWhiteSpace(input))
            {
                throw new ArgumentNullException(nameof(input));
            }

            return input.Replace("'", "''").ContainsAny(_injectionKeyWords, StringComparison.OrdinalIgnoreCase);
        }
    }
}