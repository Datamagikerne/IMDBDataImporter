using IMDBDataImporter.Models;
using System.Data.SqlClient;

namespace IMDBDataImporter.Inserters
{
    public class ProfessionInserter
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
            using (SqlCommand insertNameProfCmd = new SqlCommand(
            "INSERT INTO Names_Professions (nconst, profession_id) VALUES (@nconst, @profession_id)", sqlConn))
            {
                foreach (Name myName in nameList)
                {
                    foreach (string prof in myName.primaryProfession)
                    {
                        insertNameProfCmd.Parameters.AddWithValue("@nconst", myName.nconst);
                        insertNameProfCmd.Parameters.AddWithValue("@profession_id", professionsDict[prof]);
                        insertNameProfCmd.ExecuteNonQuery();
                        insertNameProfCmd.Parameters.Clear();
                    }
                }
            }
            
        }
    }
}