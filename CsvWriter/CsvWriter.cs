using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
//using DownloadFilePlan.CWSDocumentManagement;

namespace DownloadFilePlan
{
    public class CsvWriter : DataTable
    {
        public static DataTable createTable(string token, int highestlevel)
        {
            DataTable resultTable = new DataTable();
            resultTable.Columns.Add("Node ID");
            for (int i = 1; i <= highestlevel; i++)
            {
                resultTable.Columns.Add("Level " + i);
            }
            resultTable.Columns.Add("Folder");
            resultTable.Columns.Add("Path");
            resultTable.Columns.Add("Description");
            resultTable.Columns.Add("Owner");
            resultTable.Columns.Add("OwnerGroup");
            resultTable.Columns.Add("ACL");
            resultTable.Columns.Add("See");
            resultTable.Columns.Add("See Contents");
            resultTable.Columns.Add("Modify");
            resultTable.Columns.Add("Edit Attributes");
            resultTable.Columns.Add("Add Items");
            resultTable.Columns.Add("Reserve");
            resultTable.Columns.Add("Delete Versions");
            resultTable.Columns.Add("Delete");
            resultTable.Columns.Add("Edit Permission");
            resultTable.Columns.Add("Category Header");
            resultTable.Columns.Add("Category Values");
            return resultTable;
        }

        public static DataTable createLongPathTable(int highestlevel)
        {
            DataTable resultTable = new DataTable();
            resultTable.Columns.Add("Node ID");
            for (int i = 1; i <= highestlevel; i++)
            {
                resultTable.Columns.Add("Level " + i);
            }
            resultTable.Columns.Add("Folder");
            resultTable.Columns.Add("Path");
            resultTable.Columns.Add("Description");
            resultTable.Columns.Add("Owner");
            resultTable.Columns.Add("OwnerGroup");
            resultTable.Columns.Add("ACL");
            resultTable.Columns.Add("See");
            resultTable.Columns.Add("See Contents");
            resultTable.Columns.Add("Modify");
            resultTable.Columns.Add("Edit Attributes");
            resultTable.Columns.Add("Add Items");
            resultTable.Columns.Add("Reserve");
            resultTable.Columns.Add("Delete Versions");
            resultTable.Columns.Add("Delete");
            resultTable.Columns.Add("Edit Permission");
            resultTable.Columns.Add("Category Header");
            resultTable.Columns.Add("Category Values");
            return resultTable;
        }
        public static DataTable CreateNewFormatTable(int highestlevel)
        {
            DataTable resultTable = new DataTable();
            resultTable.Columns.Add("Source");
            resultTable.Columns.Add("Source DocLib");
            resultTable.Columns.Add("Source SubFolder");
            return resultTable;
        }
        private static DataTable StripEmptyRows(DataTable dt)
        {
            List<int> rowIndexesToBeDeleted = new List<int>();
            int indexCount = 0;
            foreach (var row in dt.Rows)
            {
                var r = (DataRow)row;
                int emptyCount = 0;
                int itemArrayCount = r.ItemArray.Length;
                foreach (var i in r.ItemArray) if (string.IsNullOrWhiteSpace(i.ToString())) emptyCount++;

                if (emptyCount == itemArrayCount) rowIndexesToBeDeleted.Add(indexCount);

                indexCount++;
            }

            int count = 0;
            foreach (var i in rowIndexesToBeDeleted)
            {
                dt.Rows.RemoveAt(i - count);
                count++;
            }

            return dt;
        }

        public static void ToCSV(DataTable dtDataTable, string strFilePath)
        {
            dtDataTable = StripEmptyRows(dtDataTable);
            RemoveExcludedColumns(dtDataTable);

            using (StreamWriter sw = new StreamWriter(strFilePath, false))
            {
                try
                {
                    WriteHeaders(sw, dtDataTable);
                    WriteRows(sw, dtDataTable);
                }
                catch
                {
                    Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Error Saving to CSV");
                }
            }
        }

        private static void RemoveExcludedColumns(DataTable dt)
        {
            string[] valuesToRetrieve = Properties.Settings.Default.ValuesToRetrieve.Split(',');

            if (!valuesToRetrieve.Contains("Permissions"))
            {
                RemoveColumns(dt, new[] { "Owner", "OwnerGroup", "ACL", "See", "See Contents", "Modify", "Edit Attributes", "Add Items", "Reserve", "Delete Versions", "Delete", "Edit Permission" });
            }

            if (!valuesToRetrieve.Contains("Attributes"))
            {
                RemoveColumns(dt, new[] { "Category Header", "Category Values" });
            }
        }

        private static void RemoveColumns(DataTable dt, string[] columns)
        {
            foreach (string col in columns)
            {
                if (dt.Columns.Contains(col))
                {
                    dt.Columns.Remove(col);
                }
            }
        }

        private static void WriteHeaders(StreamWriter sw, DataTable dt)
        {
            for (int i = 0; i < dt.Columns.Count; i++)
            {
                sw.Write(dt.Columns[i]);
                if (i < dt.Columns.Count - 1)
                {
                    sw.Write(",");
                }
            }
            sw.Write(sw.NewLine);
        }

        private static void WriteRows(StreamWriter sw, DataTable dt)
        {
            foreach (DataRow dr in dt.Rows)
            {
                for (int i = 0; i < dt.Columns.Count; i++)
                {
                    if (!Convert.IsDBNull(dr[i]))
                    {
                        sw.Write(FormatCsvValue(dr[i].ToString()));
                    }
                    if (i < dt.Columns.Count - 1)
                    {
                        sw.Write(",");
                    }
                }
                sw.Write(sw.NewLine);
            }
        }

        private static string FormatCsvValue(string value)
        {
            if (value.Contains(','))
            {
                return value.Replace(',', '﹐');
            }
            else if (value.Contains('"'))
            {
                return String.Format("\"\"{0}\"\"", value);
            }
            return value;
        } 
    }
}
