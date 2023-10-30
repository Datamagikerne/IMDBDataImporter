using System.Data;
using System.Data.SqlClient;

namespace IMDBDataImporter.Inserters
{
    public abstract class Inserter
    {
        public static int amountToTake = 100000;

        public static HashSet<string> GetValidTconst(SqlConnection sqlConn)
        {
            HashSet<string> validTconsts = new HashSet<string>();
            SqlCommand getValidTconst = new SqlCommand("SELECT tconst FROM Titles", sqlConn);
            using (SqlDataReader reader = getValidTconst.ExecuteReader())
            {
                while (reader.Read())
                {
                    validTconsts.Add(reader.GetString(0));
                }
            }

            return validTconsts;
        }

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