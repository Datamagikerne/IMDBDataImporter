using IMDBDataImporter.Models;
using System.Data.SqlClient;

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
            using (SqlCommand insertGenreCmd = new SqlCommand(
            "INSERT INTO Genres (genreName) OUTPUT INSERTED.genre_id VALUES (@genreName)", sqlConn))
            {
                foreach (string genre in genres)
                {
                    insertGenreCmd.Parameters.AddWithValue("@genreName", genre);
                    int newId = (int)insertGenreCmd.ExecuteScalar();
                    genreDict.Add(genre, newId);
                    insertGenreCmd.Parameters.Clear();
                }
            }

            using (SqlCommand insertTitleGenreCmd = new SqlCommand(
            "INSERT INTO Titles_Genres (tconst, genre_id) VALUES (@tconst, @genreId)", sqlConn))
            {
                foreach (Title myTitle in titleList)
                {
                    foreach (string genre in myTitle.genres)
                    {
                        insertTitleGenreCmd.Parameters.AddWithValue("@tconst", myTitle.tconst);
                        insertTitleGenreCmd.Parameters.AddWithValue("@genreId", genreDict[genre]);
                        insertTitleGenreCmd.ExecuteNonQuery();
                        insertTitleGenreCmd.Parameters.Clear();
                    }
                }
            }
        }
    }
}