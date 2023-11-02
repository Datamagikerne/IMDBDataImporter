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
                        InsertTitleNamesValues(values[1], values[0], titleNamesTable, validNconsts, existingTitleNames);
                        InsertTitleNamesValues(values[2], values[0], titleNamesTable, validNconsts, existingTitleNames);
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

        private static void InsertTitleNamesValues(string value, string tconst, DataTable titleNamesTable, HashSet<string> validNconsts, HashSet<string> existingTitleNames)
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
        
    }
}
