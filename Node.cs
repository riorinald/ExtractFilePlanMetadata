using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DownloadFilePlan
{
    public class Node
    {
        public long ID { get; set; }
        public string Comment { get; set; }
        public long ParentID { get;set; }
        public string Name { get;set; }
        public string DisplayType { get; set; }
        public DateTime CreationDate { get;set; }
        public DateTime ModifiedDate { get; set; }
        public string CreatedBy { get; set; }
        public string ModifiedBy { get; set; }
        public int SubType { get; set; }
        public string SubTypeName { get; set; }
    }
}
