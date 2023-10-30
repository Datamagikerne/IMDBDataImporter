using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBDataImporter.Inserters
{
    public class CrewInserter : Inserter
    {

        public static void ReadData(string ConnString, string crew_data)
        {
            DateTime before = DateTime.Now;
            Console.WriteLine("Starting crew");

            using (SqlConnection sqlConn = new SqlConnection(ConnString))
            {
                sqlConn.Open();

                HashSet<string> validTconsts = GetValidTconst(sqlConn);
                HashSet<string> validNconsts = GetValidNconsts(sqlConn);
                HashSet<string> existingTitleNames = GetExistingTitleNames(sqlConn);

                DataTable titleNamesTable = new DataTable("Title_Names");
                titleNamesTable.Columns.Add("titles_names_id", typeof(int)).AutoIncrement = true;
                titleNamesTable.Columns.Add("tconst", typeof(string));
                titleNamesTable.Columns.Add("nconst", typeof(string));

                foreach (string line in File.ReadLines(crew_data).Skip(1).Take(amountToTake))
                {
                    string[] values = line.Split('\t');

                    if (validTconsts.Contains(values[0]))
                    {
                        InsertNconstValues(values[1], values[0], titleNamesTable, validNconsts, existingTitleNames);
                        InsertNconstValues(values[2], values[0], titleNamesTable, validNconsts, existingTitleNames);
                    }
                }

                SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepNulls, null);
                bulkCopy.DestinationTableName = "Title_Names";
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.WriteToServer(titleNamesTable);
            }

            DateTime after = DateTime.Now;
            Console.WriteLine("Time elapsed: " + (after - before));
        }

        private static void InsertNconstValues(string value, string tconst, DataTable titleNamesTable, HashSet<string> validNconsts, HashSet<string> existingTitleNames)
        {
            string[] nconstValues = value.Split('\t');

            foreach (string ncon in nconstValues)
            {
                if (validNconsts.Contains(ncon))
                {
                    string combinedKey = tconst + ncon;
                    if (!existingTitleNames.Contains(combinedKey))
                    {
                        DataRow titleNameRow = titleNamesTable.NewRow();
                        titleNameRow["tconst"] = tconst;
                        titleNameRow["nconst"] = ncon;
                        titleNamesTable.Rows.Add(titleNameRow);
                    }
                }
            }
        }
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
    }
}
