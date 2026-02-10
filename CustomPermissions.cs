using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DownloadFilePlan
{
    public class CustomPermissions
    {
        public long NodeID{ get; set; }

        public long RightID{ get; set; }
        public string Owner{ get; set; }
        public string UserGroupName{ get; set; }
        public string Type { get; set; }
        public long[] Permissions{ get; set; }

        public CustomPermissions()
        {
            Permissions = new long[9];
        }
    }
}
