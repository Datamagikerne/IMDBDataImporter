using IMDBDataImporter.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace IMDBDataImporter.Inserters
{
    public class NameInserter : Inserter
    {
        public static void ReadData(string ConnString, string names_data)
        {
            List<Name> names = new List<Name>();

            foreach (string line in File.ReadLines(names_data).Skip(1).Take(10000))
            {
                string[] values = line.Split("\t");
                if (values.Length == 6)
                {
                    names.Add(new Name(
                        values[0], values[1], ConvertToInt(values[2]),
                        ConvertToInt(values[3]), values[4], values[5]
                        ));
                }
            }
            Console.WriteLine(names.Count);
            DateTime before = DateTime.Now;
            SqlConnection sqlConn = new SqlConnection(ConnString);
            sqlConn.Open();

            InsertData(sqlConn, names);
            ProfessionInserter.InsertData(sqlConn, names);
            TitleNameInserter.InsertData(sqlConn, names);
            sqlConn.Close();

            DateTime after = DateTime.Now;

            Console.WriteLine("Tid: " + (after - before));
        }
        public static void InsertData(SqlConnection sqlConn, List<Name> names)
        {
            DataTable nameTable = new DataTable("Titles");

            nameTable.Columns.Add("nconst", typeof(string));
            nameTable.Columns.Add("primaryName", typeof(string));
            nameTable.Columns.Add("birthYear", typeof(int));
            nameTable.Columns.Add("deathYear", typeof(int));

            foreach (Name name in names)
            {
                DataRow nameRow = nameTable.NewRow();
                FillParameter(nameRow, "nconst", name.nconst);
                FillParameter(nameRow, "primaryName", name.primaryName);
                FillParameter(nameRow, "birthYear", name.birthYear);
                FillParameter(nameRow, "deathYear", name.deathYear);

                nameTable.Rows.Add(nameRow);
            }
            SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepNulls, null);
            bulkCopy.DestinationTableName = "Names";
            bulkCopy.BulkCopyTimeout = 0;
            bulkCopy.WriteToServer(nameTable);
        }
    }
}
