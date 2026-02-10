using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DownloadFilePlan
{
    internal class SecureInfo
    {
        internal static string getSensitiveInfo(string secureFileName)
        {
            string credentialsDirectory = Properties.Settings.Default.SecureCredentialsPath;
            string AESKeyFilePath = Path.Combine(credentialsDirectory, Properties.Settings.Default.SecureAESKey_Filename);
            string secureFilePath = Path.Combine(credentialsDirectory, secureFileName);
            return "";
        }
    }
}
