using System;

namespace DapperExtensions.Models
{
    public class SchemaInfo
    {
        public string COLUMN_NAME { get; set; }
        public int ORDINAL_POSITION { get; set; }
        public bool IS_NULLABLE { get; set; }
        public string DATA_TYPE { get; set; }
        public string COLUMN_DEFAULT { get; set; }

        public Type GetDataType()
        {
            if (string.IsNullOrEmpty(DATA_TYPE))
            {
                throw new NullReferenceException($"Database type was null.");
            }

            switch (DATA_TYPE.ToLower())
            {
                case "bit":
                    return typeof(bool);

                case "date":
                case "datetime":
                case "smalldatetime":
                case "datetime2":
                    return typeof(DateTime);

                case "numeric":
                case "decimal":
                case "money":
                    return typeof(decimal);

                case "float":
                    return typeof(float);

                case "tinyint":
                case "smallint":
                case "int":
                    return typeof(int);

                case "bigint":
                    return typeof(long);

                case "char":
                case "varchar":
                case "nvarchar":
                    return typeof(string);

                case "image":
                case "varbinary":
                    return typeof(byte[]);

                case "uniqueidentifier":
                    return typeof(Guid);

                default:
                    throw new Exception($"Type {DATA_TYPE} not recognized.");
            }
        }
    }
}