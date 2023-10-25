using IMDBDataImporter.Models;
using System.Data.SqlClient;
using IMDBDataImporter.Inserters;

string ConnString = "server=localhost;database=IMDB;" +
            "user id=sa;password=imdbpassword;TrustServerCertificate=True";
string titles_data = "C:\\Users\\olivi\\Documents\\olli\\4. Semester\\SQL\\IMDBDataSæt\\titles_data.tsv";


TitleInserter.ReadData(ConnString, titles_data);


