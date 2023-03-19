using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NpgsqlTypes;

namespace PostgresImporter
{
    public class AttributeTypes {
        public const string LONG_TEXT = "TEXT";
        public const string TEXT = "VARCHAR(256)";
        public const string FLOAT = "DOUBLE PRECISION";
        public const string INTEGER = "BIGINT";
        public const string DATE = "DATE";
        public const string BOOLEAN = "BOOLEAN";
    }
}
