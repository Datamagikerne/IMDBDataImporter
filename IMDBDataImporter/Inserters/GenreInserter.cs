using IMDBDataImporter.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBDataImporter.Inserters
{
    public class GenreInserter 
    {
        public static void InsertData(SqlConnection sqlConn, List<Title> titleList)
        {
            HashSet<string> genres = new HashSet<string>();
            Dictionary<string, int> genreDict = new Dictionary<string, int>();
            foreach (var title in titleList)
            {
                foreach (var genre in title.genres)
                {
                    genres.Add(genre);
                }
            }
            foreach (string genre in genres)
            {
                SqlCommand sqlComm = new SqlCommand(
                    "INSERT INTO Genres(genreName)" +
                    "OUTPUT INSERTED.genre_id " +
                    "VALUES ('" + genre + "')", sqlConn);

                try
                {
                    SqlDataReader reader = sqlComm.ExecuteReader();
                    if (reader.Read())
                    {
                        int newId = (int)reader["genre_id"];
                        genreDict.Add(genre, newId);
                    }
                    reader.Close();
                }
                catch (Exception ex)
                {
                    throw new Exception(sqlComm.CommandText, ex);
                }

            }

            foreach (Title myTitle in titleList)
            {
                foreach (string genre in myTitle.genres)
                {
                    SqlCommand sqlComm = new SqlCommand(
                        "INSERT INTO Titles_Genres (tconst, genre_id)" +
                        " VALUES " +
                        "('" + myTitle.tconst + "', '"
                        + genreDict[genre] + "')", sqlConn);
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
