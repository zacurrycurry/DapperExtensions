using Ardalis.GuardClauses;
using System.IO;
using System.Reflection;

namespace DapperExtensions.Extensions
{
    internal static class AssemblyExtensions
    {
        internal static string GetQuery(this Assembly assembly, string resourceName)
        {
            Guard.Against.Null(resourceName, nameof(resourceName));
            return new StreamReader(assembly.GetManifestResourceStream(resourceName)).ReadToEnd();
        }
    }
}