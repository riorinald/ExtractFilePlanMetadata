using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DownloadFilePlan.Models
{
    public class NewFormat
    {
        public string Source { get; set; }
        public string SourceDocLib { get; set; }
        public string SourceSubFolder { get; set; }
        public string TargetPath { get; set; }
    }
}
