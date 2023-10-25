using IMDBDataImporter.Models;
using System.Data.SqlClient;
using IMDBDataImporter.Inserters;

string ConnString = "server=localhost;database=IMDB;" +
            "user id=sa;password=imdbpassword;TrustServerCertificate=True";
string titles_data = "C:\\temp\\IMDBDataSæt\\titles_data.tsv";
string names_data = "C:\\temp\\IMDBDataSæt\\name_data.tsv";

SqlConnection conn = new SqlConnection(ConnString);
conn.Open();

TitleInserter.ReadData(ConnString, titles_data);
NameInserter.ReadData(ConnString, names_data);



