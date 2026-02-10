using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DownloadFilePlan.Models
{
    public class InputFileExportException
    {
        public string FileID { get; set; }
        public string Status { get; set; }
        public string FileName { get; set; }
        public string ContentServerFolderPath { get; set; }
        public string DownloadedPath { get; set; }
    }
}
