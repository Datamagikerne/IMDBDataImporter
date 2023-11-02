using System.Data;
using System.Data.SqlClient;

namespace IMDBDataImporter.Inserters
{
    public abstract class Inserter
    {
        public static int amountToTake = 100000;

        public static HashSet<string> GetExistingTitleNames(SqlConnection sqlConn)
        {
            HashSet<string> existingTitleNames = new HashSet<string>();
            SqlCommand sqlCheckTNComm = new SqlCommand("SELECT tconst, nconst FROM Title_Names", sqlConn);
            using (SqlDataReader titleNamesReader = sqlCheckTNComm.ExecuteReader())
            {
                while (titleNamesReader.Read())
                {
                    string tconstValue = titleNamesReader.GetString(0);
                    string nconstValue = titleNamesReader.GetString(1);
                    existingTitleNames.Add(tconstValue + nconstValue);
                }
            }
            return existingTitleNames;
        }

        public static HashSet<string> GetValidNconsts(SqlConnection sqlConn)
        {
            HashSet<string> validNconsts = new HashSet<string>();
            SqlCommand getValidNconst = new SqlCommand("SELECT nconst FROM Names", sqlConn);
            using (SqlDataReader reader = getValidNconst.ExecuteReader())
            {
                while (reader.Read())
                {
                    validNconsts.Add(reader.GetString(0));
                }
            }

            return validNconsts;
        }

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