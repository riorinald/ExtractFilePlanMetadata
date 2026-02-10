using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DownloadFilePlan.Models
{
    public class FailedLogs
    {
        public string FileID { get; set; }
        public string FileName { get; set; }
        public string FolderPath { get; set; }
        public string ErrorMessage { get; set; }
    }
}
