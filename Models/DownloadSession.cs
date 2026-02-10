using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DownloadFilePlan.Models
{
    public class DownloadSession
    {
        public long Id { get; set; }
        public string FilePath { get; set; }
        public string CSVFilePath { get; set; }
        public bool EncryptedDownloads { get; set; }
        public string CSPKeyName { get; set; }
    }
}
