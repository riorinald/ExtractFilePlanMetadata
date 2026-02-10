using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DownloadFilePlan.Models
{
    public class SGateData
    {
        [Display(Name = "SourcePath")]
        public string SourcePath { get; set; }
        [Display(Name = "DestinationPath")]
        public string DestinationPath { get; set; }
        public string DRSCreatedBy { get; set; }
        public string DRSCreatedDate { get; set; }
        public string DRSModifiedBy { get; set; }
        public string DRSModifiedDate { get; set; }
       // public string Version { get; set; }
    }
}