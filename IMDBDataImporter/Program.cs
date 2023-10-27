using IMDBDataImporter.Inserters;

//husk at ændre connection string og stigen til data dokumenter
//Bruger nuget packet System.Data.SqlClient
//amount to take i Inserter Klassen kan gøres mindre hvis det tager for lang tid
//50000 tager ca 2.30 min for mig
string ConnString = "server=localhost;database=IMDB;" +
            "user id=sa;password=;TrustServerCertificate=True";
string titles_data = "C:\\temp\\IMDBDataSæt\\titles_data.tsv";
string names_data = "C:\\temp\\IMDBDataSæt\\name_data.tsv";
string crew_data = "C:\\temp\\IMDBDataSæt\\crew_data.tsv";

DateTime before = DateTime.Now;

TitleInserter.ReadData(ConnString, titles_data);
NameInserter.ReadData(ConnString, names_data);
CrewInserter.ReadData(ConnString, crew_data);

DateTime after = DateTime.Now;

Console.WriteLine("Tid i alt: " + (after - before));