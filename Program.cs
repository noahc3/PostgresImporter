using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using CsvHelper;
using Npgsql;
using NpgsqlTypes;

namespace PostgresImporter
{
    internal class Program
    {
        static string CONNECTION_STRING = "Host={0};Port={1};Username={2};Password={3};Database={4};Include Error Detail=true";
        const int SHORT_TEXT_MAXIMUM = 256;
        const char DELIMITER = ',';
        const int IMPORT_BUF_SIZE = 9877;

        static void Main(string[] args)
        {
            if (args.Length < 6) {
                Console.WriteLine("Usage: PostgresImporter.exe <host> <port> <username> <password> <database> <path to directory with csv files>");
                return;
            }

            CONNECTION_STRING = String.Format(CONNECTION_STRING, args[0], args[1], args[2], args[3], args[4]);

            List<FileInfo> paths = new List<FileInfo>();
            DirectoryInfo tablesDir = new DirectoryInfo(args[0]);
            foreach (FileInfo file in tablesDir.GetFiles())
            {
                if (file.Extension.ToLower() == ".csv")
                {
                    paths.Add(file);
                }
            }

            foreach (FileInfo path in paths)
            {
                ImportTable(path);
            }
        }

        static void ImportTable(FileInfo file)
        {
            RelationMeta meta = AnalyzeTable(file);

            using (var conn = new Npgsql.NpgsqlConnection(CONNECTION_STRING))
            {
                conn.Open();
                using (var cmd = new Npgsql.NpgsqlCommand(meta.DropStatement, conn))
                {
                    cmd.ExecuteNonQuery();
                }
                using (var cmd = new Npgsql.NpgsqlCommand(meta.CreateStatement, conn))
                {
                    cmd.ExecuteNonQuery();
                }

                using (var reader = new StreamReader(file.FullName))
                using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
                {
                    csv.Read();
                    csv.ReadHeader();
                    string[] headers = csv.HeaderRecord!;

                    using (var writer = conn.BeginBinaryImport(meta.CopyStatement))
                    {
                        while (csv.Read())
                        {
                            writer.StartRow();
                            for (int i = 0; i < headers.Length; i++)
                            {
                                string value = csv.GetField<string>(i)!;
                                if (String.IsNullOrWhiteSpace(value))
                                {
                                    writer.WriteNull();
                                    continue;
                                }
                                switch (meta.Types[i])
                                {
                                    case AttributeTypes.LONG_TEXT:
                                        writer.Write(value, NpgsqlDbType.Text);
                                        break;
                                    case AttributeTypes.TEXT:
                                        writer.Write(value, NpgsqlDbType.Varchar);
                                        break;
                                    case AttributeTypes.FLOAT:
                                        writer.Write(Double.Parse(value), NpgsqlDbType.Double);
                                        break;
                                    case AttributeTypes.INTEGER:
                                        writer.Write(Int64.Parse(value), NpgsqlDbType.Bigint);
                                        break;
                                    case AttributeTypes.DATE:
                                        writer.Write(value.IsWeirdDateFast() ? DateTime.ParseExact(value, "M/d/yy", System.Globalization.CultureInfo.InvariantCulture) : DateTime.Parse(value), NpgsqlDbType.Date);
                                        break;
                                    case AttributeTypes.BOOLEAN:
                                        writer.Write(bool.Parse(value), NpgsqlDbType.Boolean);
                                        break;
                                }
                            }
                        }
                        writer.Complete();
                    }
                }
            }
        }

        static RelationMeta AnalyzeTable(FileInfo file)
        {
            string tableName = file.Name.Substring(0, file.Name.Length - file.Extension.Length);
            Console.WriteLine($"Analyzing {tableName}...");

            string[] headers;
            int[] typeProperties;
            using (var reader = new StreamReader(file.FullName))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                csv.Read();
                csv.ReadHeader();
                headers = csv.HeaderRecord!;
                typeProperties = new int[headers.Length];

                while (csv.Read())
                {
                    for (int i = 0; i < headers.Length; i++)
                    {
                        string value = csv.GetField<string>(i)!;
                        typeProperties[i] |= AnalyzeValue(value);
                    }
                }
            }

            string[] attributeTypes = new string[typeProperties.Length];
            for (int i = 0; i < typeProperties.Length; i++)
            {
                attributeTypes[i] = DetermineType(typeProperties[i]);
            }

            string createStmt = GetCreateStatement(tableName, headers, attributeTypes);
            string copyStmt = GetCopyStatement(tableName, headers);
            string dropStatement = GetDropStatement(tableName);

            Console.WriteLine(createStmt + "\n");

            return new RelationMeta(tableName, headers, attributeTypes, createStmt, copyStmt, dropStatement);
        }
        

        static int AnalyzeValue(string value)
        {
            if (String.IsNullOrWhiteSpace(value)) return 0;
            if (value.Length > SHORT_TEXT_MAXIMUM) return AttributeTypeProperties.hasLongText;
            else if (int.TryParse(value, out _)) return AttributeTypeProperties.hasInt;
            else if (double.TryParse(value, out _)) return AttributeTypeProperties.hasFloat;
            else if (value.ToLower() == "true" || value.ToLower() == "false") return AttributeTypeProperties.hasBoolean;
            else if (value.IsWeirdDate() || DateTime.TryParse(value, out _)) return AttributeTypeProperties.hasDate;
            else return AttributeTypeProperties.hasText;
        }

        static string DetermineType(int properties) {
            if ((properties & AttributeTypeProperties.hasLongText) != 0) {
                return AttributeTypes.LONG_TEXT;
            } else if ((properties & AttributeTypeProperties.hasText) != 0) {
                return AttributeTypes.TEXT;
            } else if ((properties & AttributeTypeProperties.hasFloat) != 0) {
                return AttributeTypes.FLOAT;
            } else if ((properties & AttributeTypeProperties.hasInt) != 0) {
                return AttributeTypes.INTEGER;
            } else if ((properties & AttributeTypeProperties.hasDate) != 0) {
                return AttributeTypes.DATE;
            } else if ((properties & AttributeTypeProperties.hasBoolean) != 0) {
                return AttributeTypes.BOOLEAN;
            } else {
                throw new Exception("No type found");
            }
        }


        static string GetCreateStatement(string tableName, string[] attributes, string[] attributeTypes) {
            string pk = IdentifyPrimaryKey(attributes);
            string statement = $"CREATE TABLE {tableName} (";
            for (int i = 0; i < attributes.Length; i++) {
                statement += $"\"{attributes[i]}\" {attributeTypes[i]}";
                if (attributes[i] == pk) {
                    statement += " PRIMARY KEY";
                }
                if (i < attributes.Length - 1) {
                    statement += ", ";
                }
            }
            statement += ");";
            return statement;
        }

        static string GetCopyStatement(string tableName, string[] attributes) {
            string statement = $"COPY {tableName} (";
            for (int i = 0; i < attributes.Length; i++) {
                statement += "\"" + attributes[i] + "\"";
                if (i < attributes.Length - 1) {
                    statement += ", ";
                }
            }
            statement += ") FROM STDIN (FORMAT BINARY);";
            return statement;
        }

        static string GetDropStatement(string tableName) {
            return $"DROP TABLE IF EXISTS {tableName} CASCADE;";
        }

        static string IdentifyPrimaryKey(string[] attributes) {
            for (int i = 0; i < attributes.Length; i++) {
                if (attributes[i].ToLower() == "id") {
                    return attributes[i];
                }
            }
            
            return "-";
        }
    }
}
