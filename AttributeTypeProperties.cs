using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostgresImporter
{
    public class AttributeTypeProperties {
        public const int hasLongText = 0x1;
        public const int hasText = 0x2;
        public const int hasFloat = 0x4;
        public const int hasInt = 0x8;
        public const int hasDate = 0x10;
        public const int hasBoolean = 0x20;
        public const int hasWeirdDate = 0x40;
    }
}
