using IMDBDataImporter.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace IMDBDataImporter.Inserters
{
    public class TitleNameInserter :Inserter
    {
        public static void InsertData(SqlConnection sqlConn, List<Name> nameList)
        {
            try
            {
                // Create a DataTable to hold the data
                DataTable titleNamesTable = new DataTable("Title_Names");
                titleNamesTable.Columns.Add("titles_names_id", typeof(int)).AutoIncrement = true;
                titleNamesTable.Columns.Add("tconst", typeof(string));
                titleNamesTable.Columns.Add("nconst", typeof(string));

                // Populate the DataTable, skipping rows with NULL nconst values
                foreach (Name name in nameList)
                {
                    foreach (string tcon in name.knownForTitles)
                    {
                        if (name.nconst == null)
                        {
                            Console.WriteLine(tcon);
                        }
                        DataRow titleNameRow = titleNamesTable.NewRow();
                        FillParameter(titleNameRow, "tconst", tcon);
                        FillParameter(titleNameRow, "nconst", name.nconst);
                        if(titleNameRow == null)
                        {
                            Console.WriteLine(  );
                        }
                        titleNamesTable.Rows.Add(titleNameRow);

                    }
                }

                // Use SQLBulkCopy for efficient batch inserts

                SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepNulls, null);

                bulkCopy.DestinationTableName = "Title_Names";
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.WriteToServer(titleNamesTable);

            }
            catch (Exception ex)
            {
                // Handle exceptions
                Console.WriteLine("Error: " + ex.Message);
            }
            //List<SqlCommand> insertCommands = new List<SqlCommand>();

            //foreach (Name name in nameList)
            //{
            //    foreach(string tcon in name.knownForTitles)
            //    {
            //        SqlCommand selectCmd = new SqlCommand(
            //            $"Select tconst from Titles where tconst = '{tcon}'", sqlConn);

            //        SqlCommand sqlComm = new SqlCommand(
            //            "INSERT INTO Title_Names (tconst, nconst)" +
            //            " VALUES " +
            //            "('" + tcon + "', '"
            //            + name.nconst+ "')", sqlConn);
            //        try
            //        {
            //            SqlDataReader reader = selectCmd.ExecuteReader();
            //            if (reader.Read())
            //            {
            //                insertCommands.Add(sqlComm);

            //            }
            //            reader.Close();
            //        }
            //        catch (Exception ex)
            //        {
            //            throw new Exception(selectCmd.CommandText, ex);
            //        }

            //    }
            //}

            //foreach (var cmd in insertCommands)
            //{
            //    cmd.ExecuteNonQuery();
            //}
        }
    }
}
