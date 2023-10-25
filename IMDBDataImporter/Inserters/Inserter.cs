using System.Data;


namespace IMDBDataImporter.Inserters
{
    public abstract class Inserter
    {
        public static void FillParameter(DataRow row, string columnName, object? value)
        {
            if (value != null)
            {
                row[columnName] = value;
            }
            else
            {
                row[columnName] = DBNull.Value;
            }
        }
        public static bool ConvertToBool(string input)
        {
            if (input == "0")
            {
                return false;
            }
            else if (input == "1")
            {
                return true;
            }
            throw new ArgumentException(
                "Kolonne er ikke 0 eller 1, men " + input);
        }

        public static int? ConvertToInt(string input)
        {
            if (input.ToLower() == @"\n")
            {
                return null;
            }
            else
            {
                return int.Parse(input);
            }

        }
    }
}
