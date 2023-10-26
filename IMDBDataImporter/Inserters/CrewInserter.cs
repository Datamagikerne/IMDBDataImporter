using System.Data.SqlClient;

namespace IMDBDataImporter.Inserters
{
    public class CrewInserter : Inserter
    {
        public static void ReadData(string ConnString, string crew_data)
        {
            SqlConnection sqlConn = new SqlConnection(ConnString);
            DateTime before = DateTime.Now;
            Console.WriteLine("starting crew");
            sqlConn.Open();
            foreach (string line in File.ReadLines(crew_data).Skip(1).Take(amountToTake))
            {
                string[] values = line.Split("\t");

                SqlCommand sqlCheckComm = new SqlCommand(
                    "SELECT tconst FROM Titles WHERE tconst = '" + values[0] + "'", sqlConn);

                try
                {
                    SqlDataReader tconReader = sqlCheckComm.ExecuteReader();
                    if (!tconReader.Read())
                    {
                        tconReader.Close();
                        continue;
                    }
                    tconReader.Close();
                    InsertNconstValues(values[1], values[0], sqlConn);
                    InsertNconstValues(values[2], values[0], sqlConn);
                }
                catch (Exception ex)
                {
                    throw new Exception(sqlCheckComm.CommandText, ex);
                }
            }
            sqlConn.Close();
            DateTime after = DateTime.Now;

            Console.WriteLine("Tid: " + (after - before));
        }

        public static void InsertNconstValues(string value, string tconst, SqlConnection sqlConn)
        {
            string[] nconstValues = value.Split("\t");

            foreach (var ncon in nconstValues)
            {
                SqlCommand sqlCheckNameComm = new SqlCommand(
            "SELECT nconst FROM Names WHERE nconst = '" + ncon + "'", sqlConn);

                try
                {
                    SqlDataReader nconReader = sqlCheckNameComm.ExecuteReader();
                    if (!nconReader.Read())
                    {
                        nconReader.Close();
                        continue;
                    }
                    nconReader.Close();
                }
                catch (Exception ex)
                {
                    throw new Exception(sqlCheckNameComm.CommandText, ex);
                }

                SqlCommand sqlCheckTNComm = new SqlCommand(
            "SELECT titles_names_id FROM Title_Names WHERE tconst = '"
            + tconst + "' AND nconst = '" + ncon + "'", sqlConn);

                try
                {
                    SqlDataReader titleNamesReader = sqlCheckTNComm.ExecuteReader();
                    if (titleNamesReader.Read())
                    {
                        titleNamesReader.Close();
                        continue;
                    }
                    else
                    {
                        titleNamesReader.Close();
                        SqlCommand sqlInsert = new SqlCommand(
                            "INSERT INTO Title_Names(tconst, nconst) VALUES('" + tconst + "', '" + ncon + "')", sqlConn);
                        try
                        {
                            sqlInsert.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            throw new Exception(sqlInsert.CommandText, ex);
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw new Exception(sqlCheckTNComm.CommandText, ex);
                }
            }
        }
    }
}