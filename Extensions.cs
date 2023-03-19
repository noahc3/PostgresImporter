namespace PostgresImporter
{
    public static class Extensions
    {
        public static bool IsWeirdDate(this string value)
        {
            string noslash = value.Replace("/", "");
            if (value.Length - noslash.Length != 2) return false;
            foreach (char c in noslash)
            {
                if (!char.IsDigit(c)) return false;
            }
            return true;
        }

        public static bool IsWeirdDateFast(this string value) {
            return value[^3] == '/';
        }
    }
}