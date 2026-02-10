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

            string[] valuesToRetrieve = Properties.Settings.Default.ValuesToRetrieve.Split(',');

            if (!valuesToRetrieve.Contains("Permissions"))
            {
                string[] rowsToRemove = new string[] { "Owner", "OwnerGroup", "ACL", "See", "See Contents", "Modify", "Edit Attributes", "Add Items", "Reserve", "Delete Versions", "Delete", "Edit Permission" };
                
                foreach(string rowToRemove in rowsToRemove)
                {
                    if (dtDataTable.Columns.Contains(rowToRemove))
                    {
                        dtDataTable.Columns.Remove(rowToRemove);
                    }
                }
            }

            if (!valuesToRetrieve.Contains("Attributes"))
            {
                string[] rowsToRemove = new string[] { "Category Header", "Category Values" };
                foreach (string rowToRemove in rowsToRemove) { 
                    if (dtDataTable.Columns.Contains(rowToRemove))
                    {
                        dtDataTable.Columns.Remove(rowToRemove);
                    }
                }
            }

            StreamWriter sw = new StreamWriter(strFilePath, false);
            try
            {
                //headers  
                for (int i = 0; i < dtDataTable.Columns.Count; i++)
                {
                    sw.Write(dtDataTable.Columns[i]);
                    if (i < dtDataTable.Columns.Count - 1)
                    {
                        sw.Write(",");
                    }
                }
                sw.Write(sw.NewLine);
                foreach (DataRow dr in dtDataTable.Rows)
                {
                    for (int i = 0; i < dtDataTable.Columns.Count; i++)
                    {
                        if (!Convert.IsDBNull(dr[i]))
                        {
                            string value = dr[i].ToString();
                            if (value.Contains(','))
                            {
                                value = value.Replace(',', '﹐');
                                sw.Write(value);
                            }
                            else if (value.Contains('"'))
                            {
                                value = String.Format("\"\"{0}\"\"", value);
                                sw.Write(value);
                            }
                            else
                            {
                                sw.Write(dr[i].ToString());
                            }
                        }
                        if (i < dtDataTable.Columns.Count - 1)
                        {
                            sw.Write(",");
                        }
                    }
                    sw.Write(sw.NewLine);
                }
            }
            catch
            {
                Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Error Saving to CSV");
            }
            finally
            {
                sw.Close();
            }
        } 
    }
}
