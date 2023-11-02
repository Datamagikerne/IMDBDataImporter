using IMDBDataImporter.Models;
using System.Data;
using System.Data.SqlClient;
using System.Xml.Linq;

namespace IMDBDataImporter.Inserters
{
    public class GenreInserter : Inserter
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
            InsertTitleGenre(sqlConn, titleList, genreDict);
        }
        public static void InsertTitleGenre(SqlConnection sqlConn, List<Title> titleList, Dictionary<string, int> genreDict)
        {
            try
            {
                DataTable titleGenresTable = new DataTable("Titles_Genres");
                titleGenresTable.Columns.Add("title_genre_id", typeof(int)).AutoIncrement = true;
                titleGenresTable.Columns.Add("tconst", typeof(string));
                titleGenresTable.Columns.Add("genre_id", typeof(int));

                foreach(Title title in titleList)
                {
                    foreach(string genre in title.genres)
                    {
                        DataRow titleGenreRow = titleGenresTable.NewRow();
                        FillParameter(titleGenreRow, "tconst", title.tconst);
                        FillParameter(titleGenreRow, "genre_id", genreDict[genre]);
                        titleGenresTable.Rows.Add(titleGenreRow);
                    }
                }
                SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepNulls, null);

                bulkCopy.DestinationTableName = "Titles_Genres";
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.WriteToServer(titleGenresTable);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }
    }
}