using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DownloadFilePlan
{
    public class JsonNode
    {
        public long ParentID { get; set; }
        public long ID { get; set; }
        public string name { get; set; }
        public string DisplayType { get; set; }
        public long Version { get; set; }
        public bool Downloaded { get; set; }
    }
}
