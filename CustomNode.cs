using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
//using DownloadFilePlan.CWSDocumentManagement;

namespace DownloadFilePlan
{
    public class CustomNode
    {
        public Node Node { get; set; }
        public string Comment { get; set; }
        public long Version { get; set; }
        public string TranslatedDisplayType { get; set; }
        public List<KeyValuePair<int, string>> CatValues { get; set; }
        public KeyValuePair<int, string> CatValuesKvp { get; set; }
        public List<CustomPermissions> Permissions { get; set; }
        public string Path { get; set; }
        public string MimeType { get; set; }
        public bool Downloaded { get; set; }
        public CustomNode()
        {
            Node = new Node();
            Node.Comment = "";
            CatValues = new List<KeyValuePair<int, string>>();
            Permissions = new List<CustomPermissions>();
            Version = 0;
            TranslatedDisplayType = "";
            Downloaded = false;
        }
    }
}
