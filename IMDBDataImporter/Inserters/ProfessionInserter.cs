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
            foreach (string profession in professions)
            {
                SqlCommand sqlComm = new SqlCommand(
                    "INSERT INTO Professions(professionName)" +
                    "OUTPUT INSERTED.profession_id " +
                    "VALUES ('" + profession + "')", sqlConn);

                try
                {
                    SqlDataReader reader = sqlComm.ExecuteReader();
                    if (reader.Read())
                    {
                        int newId = (int)reader["profession_id"];
                        professionsDict.Add(profession, newId);
                    }
                    reader.Close();
                }
                catch (Exception ex)
                {
                    throw new Exception(sqlComm.CommandText, ex);
                }
            }

            foreach (Name myName in nameList)
            {
                foreach (string profession in myName.primaryProfession)
                {
                    string query = "INSERT INTO Names_Professions (nconst, profession_id) VALUES ('" + myName.nconst + "', '" + professionsDict[profession] + "')";

                    SqlCommand sqlComm = new SqlCommand(query, sqlConn);
                    try
                    {
                        sqlComm.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        throw new Exception(sqlComm.CommandText, ex);
                    }
                }
            }
        }
    }
}