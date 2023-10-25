using System.Data;


namespace IMDBDataImporter.Inserters
{
    public abstract class Inserter
    {
        public static void FillParameter(DataRow row, string columnName, object? value)
        {
            if (value != null)
            {
                row[columnName] = value;
            }
            else
            {
                row[columnName] = DBNull.Value;
            }
        }
    }
}
