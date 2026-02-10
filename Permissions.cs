using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DownloadFilePlan
{
    class Permissions
    {
        public List<string> Rights { get; set; } = new List<string>();
        public long RightID { get; set; }
        public string RightType { get; set; }

    }
}
