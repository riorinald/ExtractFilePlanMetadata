using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DownloadFilePlan.Models
{
    public class InputFileExport
    {
        [Display(Name = "File ID")]
        public string FileID { get; set; }
        [Display(Name = "File Name")]
        public string FileName { get; set; }
        [Display(Name = "Content Server Folder Path")]
        public string ContentServerFolderPath { get; set; }
        [Display(Name = "Downloaded Path")]
        public string DownloadedPath { get; set; }

        public string Categories { get; set; }
    }
}
