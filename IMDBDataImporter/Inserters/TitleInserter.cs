using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using IMDBDataImporter.Models;


namespace IMDBDataImporter.Inserters
{
    public class TitleInserter : Inserter
    {
        public static void InsertData(SqlConnection sqlConn, List<Title> titles)
        {
            DataTable titleTable = new DataTable("Titles");

            titleTable.Columns.Add("tconst", typeof(string));
            titleTable.Columns.Add("titleType", typeof(string));
            titleTable.Columns.Add("primaryTitle", typeof(string));
            titleTable.Columns.Add("originalTitle", typeof(string));
            titleTable.Columns.Add("isAdult", typeof(bool));
            titleTable.Columns.Add("startYear", typeof(int));
            titleTable.Columns.Add("endYear", typeof(int));
            titleTable.Columns.Add("runtimeMinutes", typeof(int));

            foreach (Title title in titles)
            {
                DataRow titleRow = titleTable.NewRow();
                FillParameter(titleRow, "tconst", title.tconst);
                FillParameter(titleRow, "titleType", title.titleType);
                FillParameter(titleRow, "primaryTitle", title.primaryTitle);
                FillParameter(titleRow, "originalTitle", title.originalTitle);
                FillParameter(titleRow, "isAdult", title.isAdult);
                FillParameter(titleRow, "startYear", title.startYear);
                FillParameter(titleRow, "endYear", title.endYear);
                FillParameter(titleRow, "runtimeMinutes", title.runtimeMinutes);
                titleTable.Rows.Add(titleRow);
            }
            SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn,SqlBulkCopyOptions.KeepNulls, null);
            bulkCopy.DestinationTableName = "Titles";
            bulkCopy.BulkCopyTimeout = 0;
            bulkCopy.WriteToServer(titleTable);
        }



        

    }
}
