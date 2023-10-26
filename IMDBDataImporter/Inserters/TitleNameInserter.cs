using IMDBDataImporter.Models;
using System.Data;
using System.Data.SqlClient;

namespace IMDBDataImporter.Inserters
{
    public class TitleNameInserter : Inserter
    {
        public static void InsertData(SqlConnection sqlConn, List<Name> nameList)
        {
            try
            {
                DataTable titleNamesTable = new DataTable("Title_Names");
                titleNamesTable.Columns.Add("titles_names_id", typeof(int)).AutoIncrement = true;
                titleNamesTable.Columns.Add("tconst", typeof(string));
                titleNamesTable.Columns.Add("nconst", typeof(string));

                foreach (Name name in nameList)
                {
                    foreach (string tcon in name.knownForTitles)
                    {
                        SqlCommand sqlCheckComm = new SqlCommand(
                    "SELECT tconst FROM Titles WHERE tconst = '" + tcon + "'", sqlConn);

                        try
                        {
                            SqlDataReader reader = sqlCheckComm.ExecuteReader();
                            if (reader.Read())
                            {
                                DataRow titleNameRow = titleNamesTable.NewRow();
                                FillParameter(titleNameRow, "tconst", tcon);
                                FillParameter(titleNameRow, "nconst", name.nconst);
                                titleNamesTable.Rows.Add(titleNameRow);
                            }

                            reader.Close();
                        }
                        catch (Exception ex)
                        {
                            throw new Exception(sqlCheckComm.CommandText, ex);
                        }
                    }
                }

                SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepNulls, null);

                bulkCopy.DestinationTableName = "Title_Names";
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.WriteToServer(titleNamesTable);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }
    }
}