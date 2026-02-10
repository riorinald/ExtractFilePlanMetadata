using DownloadFilePlan.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DownloadFilePlan
{
    public class JSONManager
    {
        public static void SaveSession(DownloadSession session, string filePath)
        {
            try
            {
                var json = JsonConvert.SerializeObject(session, Formatting.None);
                File.WriteAllText(filePath, json);
            }
            catch (Exception ex)
            {
                Trace.WriteLine($"Error saving session: {ex.Message}");
                // Handle or rethrow as appropriate
            }
        }

        public static DownloadSession LoadSession(string filePath)
        {
            try
            {
                if (File.Exists(filePath))
                {
                    var json = File.ReadAllText(filePath);
                    return JsonConvert.DeserializeObject<DownloadSession>(json);
                }
            }
            catch (Exception ex)
            {
                Trace.WriteLine($"Error loading session: {ex.Message}");
                // Handle or rethrow as appropriate
            }

            return new DownloadSession(); // Return a new session if loading fails
        }

        public static void SaveDownloadRecords(List<JsonNode> records, string filePath)
        {
            try
            {
                var json = JsonConvert.SerializeObject(records, Formatting.None);
                File.WriteAllText(filePath, json);
            }
            catch (Exception ex)
            {
                Trace.WriteLine($"Error saving download records: {ex.Message}");
                // Handle or rethrow as appropriate
            }
        }

        public static List<JsonNode> LoadDownloadRecords(string filePath)
        {
            try
            {
                if (File.Exists(filePath))
                {
                    var json = File.ReadAllText(filePath);
                    return JsonConvert.DeserializeObject<List<JsonNode>>(json);
                }
            }
            catch (Exception ex)
            {
                Trace.WriteLine($"Error loading download records: {ex.Message}");
                // Handle or rethrow as appropriate
            }

            return new List<JsonNode>(); // Return an empty list if loading fails
        }
    }
}
