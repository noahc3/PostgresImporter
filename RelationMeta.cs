using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostgresImporter
{
    public struct RelationMeta {
        public string Name;
        public string[] Attributes;
        public string[] Types;
        public string CreateStatement;
        public string CopyStatement;
        public string DropStatement;
        public RelationMeta(string name, string[] attributes, string[] types, string createStatement, string copyStatement, string dropStatement) {
            Name = name;
            Attributes = attributes;
            Types = types;
            CreateStatement = createStatement;
            CopyStatement = copyStatement;
            DropStatement = dropStatement;
        }
    }
}
