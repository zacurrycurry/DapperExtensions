using System.Reflection;

namespace DapperExtensions.Extensions
{
    internal static class ObjectExtensions
    {
        internal static object GetPropValue(this object src, string propName)
        {
            return src.GetType().GetProperty(propName, BindingFlags.SetProperty | BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance).GetValue(src, null);
        }
    }
}