using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DownloadFilePlan.Models
{
    public class DownloadRecord
    {
        public long Id { get; set; }
        public long SessionId { get; set; }
        public long ParentId { get; set; }
        public string Name { get; set; }
        public string DisplayType { get; set; }
        public string OTCSPath { get; set; }
        public string LocalPath { get; set; }
        public long Version { get; set; }
        public bool Downloaded { get; set; }
    }
}
