using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DownloadFilePlan
{
    public class CsvWriterHelper
    {
        private readonly object _lockObject = new object();
        private readonly string _baseReportPath;

        public CsvWriterHelper(string baseReportPath)
        {
            _baseReportPath = baseReportPath;
        }

        public void WriteRecords(IEnumerable<IDictionary<string, object>> records, string fileName)
        {
            var filePath = Path.Combine(_baseReportPath, fileName);

            lock (_lockObject)
            {
                // Get unique ordered headers
                var headers = new List<string>();
                var headerSet = new HashSet<string>();

                foreach (var rec in records)
                {
                    foreach (var key in rec.Keys)
                    {
                        if (headerSet.Add(key)) // add returns true if newly added
                            headers.Add(key);
                    }
                }

                var fileExists = File.Exists(filePath);
                var isEmptyFile = fileExists && new FileInfo(filePath).Length == 0;

                using (var writer = new StreamWriter(filePath, append: fileExists))
                using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    if (!fileExists || isEmptyFile)
                    {
                        foreach (var header in headers)
                            csv.WriteField(header);
                        csv.NextRecord();
                    }

                    foreach (var rec in records)
                    {
                        foreach (var header in headers)
                        {
                            rec.TryGetValue(header, out var value);
                            csv.WriteField(value ?? "");
                        }
                        csv.NextRecord();
                    }
                }
            }
        }

        public void WriteRecord<T>(T record, string fileName)
        {
            var filePath = Path.Combine(_baseReportPath, fileName);

            lock (_lockObject)
            {
                var fileExists = File.Exists(filePath);
                var isEmptyFile = fileExists && new FileInfo(filePath).Length == 0;

                using (var writer = new StreamWriter(filePath, append: fileExists))
                using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    // ✅ Check if the record is dynamic
                    if (record is IDictionary<string, object> dict)
                    {
                        var headers = dict.Keys.ToList();

                        if (!fileExists || isEmptyFile)
                        {
                            foreach (var h in headers)
                                csv.WriteField(h);
                            csv.NextRecord();
                        }

                        foreach (var h in headers)
                            csv.WriteField(dict[h]);
                        csv.NextRecord();
                    }
                    else
                    {
                        // Your existing logic
                        var properties = typeof(T).GetProperties();
                        var headers = properties.Select(p =>
                            p.GetCustomAttributes(typeof(DisplayAttribute), false)
                            .Cast<DisplayAttribute>()
                            .FirstOrDefault()?.Name ?? p.Name.ReplaceCamelCase()).ToList();

                        if (!fileExists || isEmptyFile)
                        {
                            csv.WriteField(headers);
                            csv.NextRecord();
                        }

                        foreach (var prop in properties)
                        {
                            var value = prop.GetValue(record)?.ToString() ?? "";
                            csv.WriteField(value);
                        }
                        csv.NextRecord();
                    }
                }
            }
        }
    }

    public static class StringExtensions
    {
        public static string ReplaceCamelCase(this string str)
        {
            return System.Text.RegularExpressions.Regex.Replace(str, "(?<!\\b)[A-Z](?=[a-z])", " $0");
        }
    }
}
