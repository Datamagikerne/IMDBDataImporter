using IMDBDataImporter.Models;
using System.Data;
using System.Data.SqlClient;

namespace IMDBDataImporter.Inserters
{
    public class ProfessionInserter : Inserter
    {
        public static void InsertData(SqlConnection sqlConn, List<Name> nameList)
        {
            HashSet<string> professions = new HashSet<string>();
            Dictionary<string, int> professionsDict = new Dictionary<string, int>();
            foreach (var name in nameList)
            {
                foreach (var profession in name.primaryProfession)
                {
                    professions.Add(profession);
                }
            }
            using (SqlCommand insertProfCmd = new SqlCommand(
            "INSERT INTO Professions(professionName) OUTPUT INSERTED.profession_id VALUES (@professionName)", sqlConn))
            {
                foreach (string prof in professions)
                {
                    insertProfCmd.Parameters.AddWithValue("@professionName", prof);
                    int newId = (int)insertProfCmd.ExecuteScalar();
                    professionsDict.Add(prof, newId);
                    insertProfCmd.Parameters.Clear();
                }
            }
            InsertNameProf(sqlConn, nameList, professionsDict);
            //using (SqlCommand insertNameProfCmd = new SqlCommand(
            //"INSERT INTO Names_Professions (nconst, profession_id) VALUES (@nconst, @profession_id)", sqlConn))
            //{
            //    foreach (Name myName in nameList)
            //    {
            //        foreach (string prof in myName.primaryProfession)
            //        {
            //            insertNameProfCmd.Parameters.AddWithValue("@nconst", myName.nconst);
            //            insertNameProfCmd.Parameters.AddWithValue("@profession_id", professionsDict[prof]);
            //            insertNameProfCmd.ExecuteNonQuery();
            //            insertNameProfCmd.Parameters.Clear();
            //        }
            //    }
            //}
            
        }
        public static void InsertNameProf(SqlConnection sqlConn, List<Name> nameList, Dictionary<string, int> professionsDict)
        {
            try
            {
                DataTable nameProfTable = new DataTable("Names_professions");
                nameProfTable.Columns.Add("names_professions_id", typeof(int)).AutoIncrement = true;
                nameProfTable.Columns.Add("nconst", typeof(string));
                nameProfTable.Columns.Add("profession_id", typeof(int));

                foreach (Name name in nameList)
                {
                    foreach (string genre in name.primaryProfession)
                    {
                        DataRow titleGenreRow = nameProfTable.NewRow();
                        FillParameter(titleGenreRow, "nconst", name.nconst);
                        FillParameter(titleGenreRow, "profession_id", professionsDict[genre]);
                        nameProfTable.Rows.Add(titleGenreRow);
                    }
                }
                SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepNulls, null);

                bulkCopy.DestinationTableName = "Names_professions";
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.WriteToServer(nameProfTable);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }
    }
}