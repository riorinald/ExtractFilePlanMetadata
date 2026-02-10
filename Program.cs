using DownloadFilePlan.Models;
using DownloadFilePlan.Properties;
using ICSharpCode.SharpZipLib.Zip;
using MimeTypes;
using Newtonsoft.Json;
using NLog;
using RestSharp;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Diagnostics;
using System.Dynamic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.ComTypes;
using System.Security.Cryptography;
using System.ServiceModel.Description;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using static System.Net.Mime.MediaTypeNames;
using Directory = System.IO.Directory;

namespace DownloadFilePlan
{
    internal class Program
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();
        public static List<(int, Node)> childfolders = new List<(int, Node)>();
        public static string error = "Detailed Error";
        public static bool errorFound = false;
        public static Node parentNode;
        public static string errorlogfilename = StartTime.ToString("ddMMyyyy-HHmmss") + "Faillog.txt";
        public static DataTable filePlanDT;
        public static DataTable longPathFileDT;
        public static bool DoEncrypt = false;
        public static RSACryptoServiceProvider rsa = null;
        public static bool downloadNested = false;
        public static Aes aes = Aes.Create();
        public static DateTime StartTime = DateTime.Now;
        private static readonly object fileLock = new object();
        private static readonly object logLock = new object();
        private static BlockingCollection<DownloadTask> downloadQueue = new BlockingCollection<DownloadTask>();
        private static ConcurrentDictionary<string, int> folderItemCounts = new ConcurrentDictionary<string, int>();

        private static async Task Main(string[] args)
        {

            Logger.Info("Application Starting");

            DoEncrypt = Settings.Default.DoEncrypt;
            aes.GenerateIV();
            aes.GenerateKey();
            rsa = new RSACryptoServiceProvider(2048);

            SetConsoleCtrlHandler(ConsoleCtrlCheck, true);

            var directory = Path.GetDirectoryName(Process.GetCurrentProcess().MainModule.FileName);
            directory = Path.Combine(directory, "Logs");

            var tokenTime = DateTime.Now;

            ConfigureNLog();

            parentNode = null;

            var faillog = Path.Combine(directory, "Faillogs");

            try
            {
                if (!Directory.Exists(faillog)) Directory.CreateDirectory(faillog);

                if (!Directory.Exists(Settings.Default.CsvOutput))
                {
                    Directory.CreateDirectory(Settings.Default.CsvOutput);
                }
                if (!Directory.Exists(Settings.Default.FilesOutput))
                {
                    Directory.CreateDirectory(Settings.Default.FilesOutput);
                }
                if (!Directory.Exists(Settings.Default.LongFilePathLocation))
                {
                    Directory.CreateDirectory(Settings.Default.LongFilePathLocation);
                    Logger.Info("long file path from app settings: " + Settings.Default.LongFilePathLocation);
                }
                if (!Directory.Exists(Settings.Default.SPOLongFilePathLocation))
                {
                    Directory.CreateDirectory(Settings.Default.SPOLongFilePathLocation);
                    Logger.Info("very long file path from app settings: " + Settings.Default.SPOLongFilePathLocation);
                }
            }
            catch (Exception ex)
            {
                Logger.Info($"{DateTime.Now}: Critical error: {ex.Message}\n{ex.StackTrace}\n");
            }

            try
            {
                var userName = "";
                var password = "";
                var login = true;                
                var dbLogin = true;
                var token = "";
                bool download = Settings.Default.Download;                

                List<CustomNode> newFolders = new List<CustomNode>();
                List<CustomNode> downloadFolders = new List<CustomNode>();
                List<CustomNode> downloadFoldersNest = new List<CustomNode>();             

                while (login)
                {
                    /*Logger.Info("Enter OTDS Username:");
                    userName = Console.ReadLine();
                    Logger.Info("Enter OTDS Password:");
                    password = ReadPassword();*/

                    if (Properties.Settings.Default.UseSecureCredentials)
                    {
                        userName = SecureInfo.getSensitiveInfo(Properties.Settings.Default.SecureOTCSUsername_FileName);
                        password = SecureInfo.getSensitiveInfo(Properties.Settings.Default.SecureOTCSPassword_FileName);
                    }
                    else
                    {
                        userName = Properties.Settings.Default.OTCS_Username;
                        password = Properties.Settings.Default.OTCS_Secret;
                    }

                    AuthAPI.SetAuthInfo(userName, password);
                    token = AuthAPI.AuthenticateUserViaOTDS();

                    if (!string.IsNullOrEmpty(token)) login = false;
                }

                Logger.Info($"Reauthenticating every {Properties.Settings.Default.RefreshAuthTokenEveryXMinutes} minutes");

                while (dbLogin)
                {
/*                    Logger.Info("Enter DB User ID:");
                    string dbUserID = Console.ReadLine();
                    Logger.Info("Enter DB User Password:");
                    string dbPassword = ReadPassword();*/

                    string dbUserID;
                    string dbPassword;
                    if (Properties.Settings.Default.UseSecureCredentials)
                    {
                        dbUserID = SecureInfo.getSensitiveInfo(Properties.Settings.Default.SecureCSDBUsername_Filename);
                        dbPassword = SecureInfo.getSensitiveInfo(Properties.Settings.Default.SecureCSDBSecret_Filename);
                    }
                    else
                    {
                        dbUserID = Properties.Settings.Default.DB_Username;
                        dbPassword = Properties.Settings.Default.DB_Secret;
                    }

                    DBAccess.SetAuthInfo(dbUserID, dbPassword);
                    if (DBAccess.ValidateConnection())
                    {
                        dbLogin = false;
                        Logger.Info("DB credentials valid");
                    }
                }

                Logger.Info("Checking dbo.fn_llpathwsg function in database");
                if (DBAccess.DoesPathFuncExist() == false)
                {
                    Logger.Info("Program stopped. Function dbo.fn_llpathwsg does not exist in database");
                    System.Environment.Exit(0);
                }
                else
                {
                    Logger.Info($"Function dbo.fn_llpathwsg exists");
                }

                //Logger.Info($"Values to retrieve: {Properties.Settings.Default.ValuesToRetrieve}");

                var dbPath = Path.Combine(Environment.CurrentDirectory, "database.sqlite");

                if (!File.Exists(dbPath))
                {
                    // Create the SQLite database file
                    SQLiteConnection.CreateFile(dbPath);
                }
                var db = new DownloadDatabase(dbPath);

                // Manage session
                DownloadSession session = ManageSession(db);
                bool isResume = false;

                if (session != null)
                {
                    isResume = true;
                    /*
                    ConsoleKey response;
                    do
                    {
                        Logger.Info("A previous download has been found. Do you want to continue from last downloaded into the same folder?");
                        Logger.Info("(Pressing n will delete the last session)");
                        Logger.Info("y/n");
                        response = Console.ReadKey(false).Key;   // true is intercept key (dont show), false is show
                        if (response != ConsoleKey.Enter)
                            Console.WriteLine();
                    }
                    while (response != ConsoleKey.Y && response != ConsoleKey.N);

                    isResume = response == ConsoleKey.Y;

                    if (!isResume)
                    {
                        // User chose to start a new session
                        db.DeleteSession();
                        db.ClearDownloadRecords();
                        session = null;
                    }
                    */
                }

                if (session == null)
                {
                    Logger.Info("Starting a new download session.");
                    session = CreateNewSession();
                    db.SetDownloadSession(session);
                }
                else
                {
                    Logger.Info("Resuming previous download session.");
                }

                var catValues = new List<string>();
                var permList = new List<string>();

                var highestlevel = 0;
                Logger.Info("Checking parent folder for all child folders");

                newFolders = DBAccess.GetAllFoldersAndFiles(Settings.Default.TargetedNode, ref highestlevel);
                if (Settings.Default.ExtractTargetedNode)
                {
                    var targetedCustomNode = DBAccess.GetNodeById(Settings.Default.TargetedNode);
                    newFolders.Insert(0, targetedCustomNode);
                }
                downloadFolders = new List<CustomNode>(newFolders);
                /*
                downloadFoldersNest = new List<CustomNode>(newFolders);
                filePlanDT = CsvWriter.createTable(token, highestlevel);
                longPathFileDT = CsvWriter.createLongPathTable(highestlevel);
                var newFormatDt = CsvWriter.CreateNewFormatTable(highestlevel);
                //TokenRefresher.checkrefresh(ref token, ref tokenTime, userName, password);
                parentNode = newFolders[0].Node;
                Logger.Info("Parsing nodes into CSV file");
                if (newFolders != null)
                {
                    var foldercount = 0;
                    var level = 0;
                    {
                        var Translator = new SubTypesTranslator();
                        parse(Settings.Default.TargetedNode, ref newFolders, level, ref foldercount, highestlevel,
                            permList, ref token, Translator, ref tokenTime, userName, password);
                    }
                }

                Logger.Info("All folders found saving to CSV file");
                var downloadDirectory = Path.Combine(Settings.Default.CsvOutput,
                    StartTime.ToString("yyyyMMdd-HHmmss") + "_FilePlanFor " + parentNode.Name + ".csv");
                var longDownloadDirectory = Path.Combine(Settings.Default.CsvOutput, StartTime.ToString("yyyyMMdd-HHmmss") + "_PathTooLongFor" + parentNode.Name + ".csv");

                CsvWriter.ToCSV(filePlanDT, downloadDirectory);
                CsvWriter.ToCSV(longPathFileDT, longDownloadDirectory);
                session.CSVFilePath = downloadDirectory;
                */

                var filePath = "";
                if (!download)
                {
                    Logger.Info("Skipping downloading records as requested.");
                }
                else if (download)
                {
                    Logger.Info("Start downloading the records...");
                    if (Directory.Exists(Settings.Default.FilesOutput))
                    {
                        // Prepare download directory
                        if (!isResume) // Start New
                        {
                            db.ClearDownloadRecords();
                            filePath = Settings.Default.FilesOutput;
                            Directory.CreateDirectory(filePath);
                            session.FilePath = filePath;
                            db.SetDownloadSession(session);
                        }
                        else // Resume
                        {
                            filePath = session.FilePath;
                            Directory.CreateDirectory(filePath);
                        }

                        if (newFolders != null)
                        {
                            var foldercount = 0;
                            var level = 0;

                            bool isUsingContainsLogic = Properties.Settings.Default.UsingContainsLogic;

                            if (isUsingContainsLogic)
                            {
                                await ProcessDownloadsAsyncStructureContains(downloadFolders, filePath, token, db, tokenTime, userName, password, faillog, isResume, StartTime);
                            }
                            else
                            {
                                await ProcessDownloadsAsyncStructureLastRecord(downloadFolders, filePath, token, db, tokenTime, userName, password, faillog, isResume, StartTime); 
                            }
                        }

                        /*
                        if (!isResume) // Start New
                        {
                            CsvWriter.ToCSV(filePlanDT, Path.Combine(Settings.Default.CsvOutput,
                                StartTime.ToString("yyyyMMdd-HHmmss") + "_FilePlanFor " + parentNode.Name + ".csv"));

                            longDownloadDirectory = Path.Combine(Settings.Default.CsvOutput, StartTime.ToString("yyyyMMdd-HHmmss") + "_PathTooLongFor" + parentNode.Name + ".csv");
                            CsvWriter.ToCSV(longPathFileDT, longDownloadDirectory);
                            if (DoEncrypt)
                            {
                                EncryptFile(Path.Combine(filePath,
                                    StartTime.ToString("yyyyMMdd-HHmmss") + "_FilePlanFor " + parentNode.Name + ".csv"));
                                //File.Copy(PrivateKeyPATH, Path.Combine(Settings.Default.PrivateKeysLocation, StartTime.ToString("yyyyMMdd-HHmmss") + "_PrivateKey_For_" + parentNode.Name));
                            }
                            //File.Delete(PrivateKeyPATH);
                            //File.Delete(PublicKeyPATH);
                        }
                        else // Resume
                        {
                            Logger.Info("Trying to copy original CSV file to downloaded folder");
                            var csvName = Path.GetFileName(session.CSVFilePath);
                            if (File.Exists(session.CSVFilePath) &&
                                !File.Exists(Path.Combine(session.FilePath, csvName)))
                            {
                                File.Copy(session.CSVFilePath, Path.Combine(session.FilePath, csvName));
                                EncryptFile(Path.Combine(session.FilePath, csvName));
                            }
                            else
                            {
                                Logger.Info("Original CSV file doesn't exist or csv in downloaded folder already exists");
                            }
                            /*
                            if (File.Exists(PrivateKeyPATH) && !File.Exists(Path.Combine(session.FilePath, "privatekey")))
                                File.Copy(PrivateKeyPATH, Path.Combine(Settings.Default.PrivateKeysLocation, StartTime.ToString("yyyyMMdd-HHmmss") + "_PrivateKey_For_" + parentNode.Name));
                            else
                            {
                                Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Original Private key doesn't exist or already exists in download folder");
                            }
                            File.Delete(PrivateKeyPATH);
                            File.Delete(PublicKeyPATH);
                            
                        }
                    */
                    }
                    else
                    {
                        Logger.Info("Unable to locate the output folder for downloading records.");
                    }
                }

                if (errorFound)
                {
                    using (var sw = new StreamWriter(System.IO.Path.Combine(faillog, errorlogfilename)))
                    {
                        sw.WriteLine(error);
                        sw.Close();
                    }
                }

                // If reach here the download process completed successfully
                db.DeleteSession();
                Logger.Info("Download process completed successfully. Session cleared.");
            }
            catch (Exception e)
            {
                Logger.Warn($"Something went wrong. Error message: {e.Message}");
                Logger.Trace($"Stack trace message: {e.StackTrace}");

                try
                {
                    var downloadDirectory = Path.Combine(Settings.Default.CsvOutput, StartTime.ToString("yyyyMMdd-HHmmss") + "_FilePlanFor" + parentNode.Name + ".csv");
                    CsvWriter.ToCSV(filePlanDT, downloadDirectory);
                    var longDownloadDirectory = Path.Combine(Settings.Default.CsvOutput, StartTime.ToString("yyyyMMdd-HHmmss") + "_PathTooLongFor" + parentNode.Name + ".csv");
                    CsvWriter.ToCSV(longPathFileDT, longDownloadDirectory);
                }
                catch (Exception exception)
                {
                    Logger.Warn($"Something went wrong. Error message: {exception.Message}");
                    Logger.Trace($"Stack trace message: {exception.StackTrace}");
                }
                Environment.Exit(1);
            }
            Environment.Exit(0);
            Logger.Info("Program finished");

            LogManager.Shutdown();
        }
        private static DownloadSession ManageSession(DownloadDatabase db)
        {
            return db.GetDownloadSession();
        }

        private static void StartNewSession(DownloadDatabase db)
        {
            db.DeleteSession();
            db.ClearDownloadRecords();
            Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Starting a new download session");
        }

        private static DownloadSession CreateNewSession()
        {
            return new DownloadSession
            {
                FilePath = "",
                CSVFilePath = "",
                EncryptedDownloads = false,
                CSPKeyName = ""
            };
        }
        private static async Task ProcessDownloadsAsyncStructureContains(List<CustomNode> downloadFolders, string baseFilePath, string token,
    DownloadDatabase db, DateTime tokenTime, string userName, string password, string faillog, bool isResume, DateTime startTime)
        {
            Logger.Info($"Using contains logic for resume feature");
            CsvWriterHelper csvWriterHelper = new CsvWriterHelper(Properties.Settings.Default.CsvOutput);
            int totalItemsProcessed = 0;
            int successfulDownloads = 0;
            int failedDownloads = 0;
            var newFormats = new List<NewFormat>();

            /*
            // Export Class
            var normalExports = new List<NormalExport>();
            var normalExportExceptions = new List<NormalExportException>();
            var inputFileExports = new List<InputFileExport>();
            var inputFileExportExceptions = new List<InputFileExportException>();
            var longPathExports = new List<LongPathExport>();
            var longPathExportExceptions = new List<LongPathExportException>();

            //// Log Class
            var successLogs = new List<SuccessLogs>();
            var failedLogs = new List<FailedLogs>();
            */

            var httpClient = new HttpClient();
            const int timeoutSeconds = 900; // 15 minutes timeout
            httpClient.Timeout = TimeSpan.FromSeconds(timeoutSeconds);

            var downloadQueue = new BlockingCollection<DownloadTask>();

            // Read the max thread count from config
            int maxThreads = Properties.Settings.Default.MaxThreads;
            if (maxThreads == 0)
            {
                maxThreads = Environment.ProcessorCount; // Default to number of processors
            }
            // Use SemaphoreSlim with configurable thread count
            var semaphore = new SemaphoreSlim(maxThreads);

            //long lastDownloadedId = isResume ? db.GetLastDownloadedRecordId() : 0;

            if (isResume)
            {
                downloadFolders = FilterOutExistingDownloads(downloadFolders, db);
            }

            var pathHandler = new PathLengthHandler(baseFilePath);
            var longFolderPath = Properties.Settings.Default.DestPath;

            // First pass: Create only actual folder structures
            foreach (var df in downloadFolders.Where(x => (x.Node.DisplayType == "0" || x.Node.DisplayType == "141" || x.Node.DisplayType == "751" || x.Node.DisplayType == "136")))
            {

                totalItemsProcessed++;
                try
                {
                    var (actualPath, isInputFolder, isSPOLong) = pathHandler.ProcessFolderPath(df);
                    if (!Directory.Exists(actualPath))
                    {
                        string folderName = CleanFilePath(df.Node.Name);
                        var isBlockedStatus = IsBlockedFolder(folderName);
                        if (isBlockedStatus.IsBlocked)
                        {
                            if (actualPath.Length > Properties.Settings.Default.PathMinLength)
                            {
                                Logger.Info($"Skipped directory creation due to path length exceeding limit: {actualPath}");
                            }
                            else
                            {
                                Directory.CreateDirectory(actualPath);
                                Logger.Info($"Created directory: {actualPath}");
                            }
                            string sanitizePath = SanitizePathForLogs(df.Path);
                            if (isInputFolder)
                            {
                                var inputFileExportException = new InputFileExportException()
                                {
                                    FileID = df.Node.ID.ToString(),
                                    Status = isBlockedStatus.Reason,
                                    FileName = df.Node.Name,
                                    ContentServerFolderPath = sanitizePath,
                                    DownloadedPath = actualPath
                                };
                                csvWriterHelper.WriteRecord(inputFileExportException, "InputFile_exception.csv");
                                //inputFileExportExceptions.Add(inputFileExportException);
                            }
                            else if (isSPOLong)
                            {
                                var longPathExportException = new LongPathExportException()
                                {
                                    FileID = df.Node.ID.ToString(),
                                    Status = isBlockedStatus.Reason,
                                    FileName = df.Node.Name,
                                    ContentServerFolderPath = sanitizePath,
                                    DownloadedPath = actualPath
                                };
                                csvWriterHelper.WriteRecord(longPathExportException, "SPOLongFilePath_exception.csv");
                                //longPathExportExceptions.Add(longPathExportException);
                            }
                            else
                            {
                                var normalExportException = new NormalExportException()
                                {
                                    FileID = df.Node.ID.ToString(),
                                    Status = isBlockedStatus.Reason,
                                    FileName = df.Node.Name,
                                    ContentServerFolderPath = sanitizePath,
                                    DownloadedPath = actualPath
                                };
                                csvWriterHelper.WriteRecord(normalExportException, "NormalExport_exception.csv");
                                //normalExportExceptions.Add(normalExportException);
                            }

                            var succesLog = new SuccessLogs()
                            {
                                FileID = df.Node.ID.ToString(),
                                FileName = df.Node.Name,
                                FolderPath = sanitizePath,
                                LocalPath = actualPath
                            };
                            // Add status for blocked folder here
                        }
                        else
                        {
                            if (actualPath.Length > Properties.Settings.Default.PathMinLength)
                            {
                                Logger.Info($"Skipped directory creation due to path length exceeding limit: {actualPath}");
                            }
                            else
                            {
                                Directory.CreateDirectory(actualPath);
                                Logger.Info($"Created directory: {actualPath}");
                            }
                            string sanitizePath = SanitizePathForLogs(df.Path);
                            var catVal = await NodesAPI.GetCategoryValue(df.Node.ID);
                            string categoriesValue = string.Join(";", catVal.Select(kv => $"{kv.Key} = {kv.Value}"));
                            if (isInputFolder)
                            {
                                //var inputFileExport = new InputFileExport()
                                //{
                                //    FileID = df.Node.ID.ToString(),
                                //    FileName = CleanFileName(df.Node.Name),
                                //    ContentServerFolderPath = sanitizePath,
                                //    DownloadedPath = actualPath,
                                //    Categories = categoriesValue
                                //};
                                //csvWriterHelper.WriteRecord(inputFileExport, "InputFile.csv");

                                dynamic recordToWrite = new ExpandoObject();
                                var recordDict = (IDictionary<string, object>)recordToWrite;
                                recordDict["FileID"] = df.Node.ID.ToString();
                                recordDict["FileName"] = CleanFileName(df.Node.Name);
                                recordDict["ContentServerFolderPath"] = sanitizePath;
                                recordDict["DownloadedPath"] = actualPath;
                                foreach (var kv in catVal)
                                {
                                    recordDict[kv.Key] = kv.Value;
                                }
                                csvWriterHelper.WriteRecord(recordToWrite, "InputFile.csv");
                                //inputFileExports.Add(inputFileExport);
                            }
                            else if (isSPOLong)
                            {
                                //var longPathExport = new LongPathExport()
                                //{
                                //    FileID = df.Node.ID.ToString(),
                                //    FileName = df.Node.Name,
                                //    ContentServerFolderPath = sanitizePath,
                                //    DownloadedPath = actualPath,
                                //    Categories = categoriesValue
                                //};
                                //csvWriterHelper.WriteRecord(longPathExport, "SPOLongFilePath.csv");

                                dynamic recordToWrite = new ExpandoObject();
                                var recordDict = (IDictionary<string, object>)recordToWrite;
                                recordDict["FileID"] = df.Node.ID.ToString();
                                recordDict["FileName"] = CleanFileName(df.Node.Name);
                                recordDict["ContentServerFolderPath"] = sanitizePath;
                                recordDict["DownloadedPath"] = actualPath;
                                foreach (var kv in catVal)
                                {
                                    recordDict[kv.Key] = kv.Value;
                                }
                                csvWriterHelper.WriteRecord(recordToWrite, "SPOLongFilePath.csv");
                                //longPathExports.Add(longPathExport);
                            }
                            else
                            {
                                //var normalExport = new NormalExport()
                                //{
                                //    FileID = df.Node.ID.ToString(),
                                //    FileName = df.Node.Name,
                                //    ContentServerFolderPath = sanitizePath,
                                //    DownloadedPath = actualPath,
                                //    Categories = categoriesValue
                                //};
                                //csvWriterHelper.WriteRecord(normalExport, "NormalExport.csv");

                                dynamic recordToWrite = new ExpandoObject();
                                var recordDict = (IDictionary<string, object>)recordToWrite;
                                recordDict["FileID"] = df.Node.ID.ToString();
                                recordDict["FileName"] = CleanFileName(df.Node.Name);
                                recordDict["ContentServerFolderPath"] = sanitizePath;
                                recordDict["DownloadedPath"] = actualPath;
                                foreach (var kv in catVal)
                                {
                                    recordDict[kv.Key] = kv.Value;
                                }
                                csvWriterHelper.WriteRecord(recordToWrite, "NormalExport.csv");
                                //normalExports.Add(normalExport);
                            }

                            var succesLog = new SuccessLogs()
                            {
                                FileID = df.Node.ID.ToString(),
                                FileName = df.Node.Name,
                                FolderPath = sanitizePath,
                                LocalPath = actualPath
                            };
                            csvWriterHelper.WriteRecord(succesLog, "SuccessLogs.csv");

                            string baseUrl = Settings.Default.SharePointURL.Trim('/');
                            string destPath = string.IsNullOrEmpty(baseUrl) ? "" : baseUrl + "/";
                            destPath += sanitizePath.Replace('\\', '/').Trim('/');
                            if (df.Node.DisplayType == "0" || df.Node.DisplayType == "751") 
                            {
                                if (!destPath.EndsWith("/")) destPath += "/";
                            }

                            var sGateData = new SGateData()
                            {
                                SourcePath = actualPath + (df.Node.DisplayType == "0" || df.Node.DisplayType == "751" ? "\\" : ""),
                                DestinationPath = destPath,
                                DRSCreatedBy = df.Node.CreatedBy,
                                DRSCreatedDate = df.Node.CreationDate.ToString("yyyy-MM-dd HH:mm:ss"),
                                DRSModifiedBy = df.Node.ModifiedBy,
                                DRSModifiedDate = df.Node.ModifiedDate.ToString("yyyy-MM-dd HH:mm:ss"),
                                //Version = df.Version.ToString()
                            };
                            csvWriterHelper.WriteRecord(sGateData, "SGateData.csv");
                            //successLogs.Add(succesLog);
                        }
                    }

                    var record = new DownloadRecord
                    {
                        Id = df.Node.ID,
                        ParentId = df.Node.ParentID,
                        Name = df.Node.Name,
                        DisplayType = df.Node.DisplayType,
                        OTCSPath = df.Path,
                        Version = df.Version,
                        Downloaded = true // Mark folders as downloaded
                    };
                    db.InsertOrUpdateRecord(record);
                }
                catch (Exception ex)
                {
                    string errMSg = $"Error creating directory structure for {df.Path}: {ex.Message}";
                    Logger.Error(errMSg);
                    Logger.Error(ex.StackTrace);
                    string sanitizePath = SanitizePathForLogs(df.Path);
                    var failedLog = new FailedLogs()
                    {
                        FileID = df.Node.ID.ToString(),
                        FileName = df.Node.Name,
                        FolderPath = sanitizePath,
                        ErrorMessage = errMSg
                    };
                    csvWriterHelper.WriteRecord(failedLog, "FailedLogs.csv");
                    //failedLogs.Add(failedLog);
                }
            }

            // Populate the download queue with files only
            foreach (var df in downloadFolders)
            {
                if ((df.Node.DisplayType == "144" || df.Node.DisplayType == "749"))
                {

                    downloadQueue.Add(new DownloadTask { Node = df });

                }
            }

            // Mark the queue as complete for adding
            downloadQueue.CompleteAdding();

            // Process the download queue
            var tasks = new List<Task>();

            // Get prefix for sharepoint destination
            string sharePointDest = Properties.Settings.Default.DestPath;

            foreach (var task in downloadQueue.GetConsumingEnumerable())
            {
                await semaphore.WaitAsync();
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        // For files, we only create the parent folder structure
                        string[] pathParts = SplitPath(task.Node.Path);
                        string targetDirectory = GetParentFolderPath(pathParts, baseFilePath, db);

                        targetDirectory = CleanFilePath(targetDirectory);

                        string fileName = CleanFileName(task.Node.Node.Name);
                        if (string.IsNullOrEmpty(GetFileExtension(fileName)))
                        {
                            if (!string.IsNullOrEmpty(task.Node.MimeType))
                            {
                                string extension;

                                // Check for special MIME types first
                                switch (task.Node.MimeType.ToLower())
                                {
                                    case "application/vnd.ms-outlook":
                                    case "application/x-msmsg":
                                    case "application/x-outlook-msg":
                                        extension = ".msg";
                                        break;
                                    case "message/rfc822":
                                        extension = ".eml";
                                        break;
                                    default:
                                        // Fall back to MimeTypeMap for standard MIME types
                                        extension = MimeTypeMap.GetExtension(task.Node.MimeType);
                                        break;
                                }

                                // Use the extension
                                if (!string.IsNullOrEmpty(extension))
                                {
                                    fileName += extension;
                                }
                            }
                        }
                        string filePath = Path.Combine(targetDirectory, fileName);

                        var (processedFilePath, isInputFile, isSPOLong) = pathHandler.ProcessFilePath(filePath, task.Node);

                        processedFilePath = CleanFilePath(processedFilePath);

                        var uniqueRes = GetUniqueFilePath(processedFilePath);

                        filePath = uniqueRes.filePath;
                        if (uniqueRes.isDuplicate)
                        {
                            /*
                            if (!db.DoesLocalPathExist(processedFilePath))
                            {
                                var record = new DownloadRecord
                                {
                                    Id = task.Node.Node.ID,
                                    ParentId = task.Node.Node.ParentID,
                                    Name = task.Node.Node.Name,
                                    DisplayType = task.Node.Node.DisplayType,
                                    OTCSPath = task.Node.Path,
                                    LocalPath = processedFilePath,
                                    Version = task.Node.Version,
                                    Downloaded = true
                                };

                                db.InsertOrUpdateRecord(record);

                                string sanitizePathForLogs = task.Node.Path.Replace(">", "/");
                                var succesLog = new SuccessLogs()
                                {
                                    FileID = task.Node.Node.ID.ToString(),
                                    FileName = task.Node.Node.Name,
                                    FolderPath = sanitizePathForLogs,
                                    LocalPath = processedFilePath
                                };
                                csvWriterHelper.WriteRecord(succesLog, "SuccessLogs.csv");
                                //successLogs.Add(succesLog);

                                successfulDownloads++;

                                var isBlockedFile = IsBlockedExtension(fileName);

                                if (isBlockedFile.IsBlocked)
                                {
                                    if (isInputFile)
                                    {
                                        string sanitizePath = task.Node.Path.Replace(">", "/");
                                        var inputFileExportException = new InputFileExportException()
                                        {
                                            FileID = task.Node.Node.ID.ToString(),
                                            Status = isBlockedFile.Reason,
                                            FileName = task.Node.Node.Name,
                                            ContentServerFolderPath = sanitizePath,
                                            DownloadedPath = processedFilePath
                                        };
                                        csvWriterHelper.WriteRecord(inputFileExportException, "InputFile_exception.csv");
                                        //inputFileExportExceptions.Add(inputFileExportException);
                                    }
                                    else if (isSPOLong)
                                    {
                                        string sanitizePath = task.Node.Path.Replace(">", "/");
                                        var longPathExportException = new LongPathExportException()
                                        {
                                            FileID = task.Node.Node.ID.ToString(),
                                            Status = isBlockedFile.Reason,
                                            FileName = task.Node.Node.Name,
                                            ContentServerFolderPath = sanitizePath,
                                            DownloadedPath = processedFilePath
                                        };
                                        csvWriterHelper.WriteRecord(longPathExportException, "SPOLongFilePath_exception.csv");
                                        //longPathExportExceptions.Add(longPathExportException);
                                    }
                                    else
                                    {
                                        string sanitizePath = task.Node.Path.Replace(">", "/");
                                        var normalExportException = new NormalExportException()
                                        {
                                            FileID = task.Node.Node.ID.ToString(),
                                            Status = isBlockedFile.Reason,
                                            FileName = task.Node.Node.Name,
                                            ContentServerFolderPath = sanitizePath,
                                            DownloadedPath = processedFilePath,
                                        };
                                        csvWriterHelper.WriteRecord(normalExportException, "NormalExport_exception.csv");
                                        //normalExportExceptions.Add(normalExportException);
                                    }
                                }
                                {
                                    if (isInputFile)
                                    {
                                        string sanitizePath = task.Node.Path.Replace(">", "/");
                                        var inputFileExport = new InputFileExport()
                                        {
                                            FileID = task.Node.Node.ID.ToString(),
                                            FileName = task.Node.Node.Name,
                                            ContentServerFolderPath = sanitizePath,
                                            DownloadedPath = processedFilePath
                                        };
                                        csvWriterHelper.WriteRecord(inputFileExport, "InputFile.csv");
                                        //inputFileExports.Add(inputFileExport);
                                    }
                                    else if (isSPOLong)
                                    {
                                        string sanitizePath = task.Node.Path.Replace(">", "/");
                                        var longPathExport = new LongPathExport()
                                        {
                                            FileID = task.Node.Node.ID.ToString(),
                                            FileName = task.Node.Node.Name,
                                            ContentServerFolderPath = sanitizePath,
                                            DownloadedPath = processedFilePath
                                        };
                                        csvWriterHelper.WriteRecord(longPathExport, "SPOLongFilePath.csv");
                                        //longPathExports.Add(longPathExport);
                                    }
                                    else
                                    {
                                        string sanitizePath = task.Node.Path.Replace(">", "/");
                                        var normalExport = new NormalExport()
                                        {
                                            FileID = task.Node.Node.ID.ToString(),
                                            FileName = task.Node.Node.Name,
                                            ContentServerFolderPath = sanitizePath,
                                            DownloadedPath = processedFilePath,
                                        };
                                        csvWriterHelper.WriteRecord(normalExport, "NormalExport.csv");
                                        //normalExports.Add(normalExport);
                                    }
                                }

                            }*/
                            task.Node.Node.Name = Path.GetFileName(uniqueRes.filePath);
                        }
                        Logger.Info("check the processed pat of a file in program: " + filePath);
                        if (!Directory.Exists(Path.GetDirectoryName(filePath)))
                        {
                            Logger.Info("check file path exist: " + Path.GetDirectoryName(filePath));
                            Directory.CreateDirectory(Path.GetDirectoryName(filePath));
                        }
                        var downloadStatus = await DownloadNodeAsync(task.Node, token, filePath, httpClient);
                        if (downloadStatus.Success)
                        {
                            var record = new DownloadRecord
                            {
                                Id = task.Node.Node.ID,
                                ParentId = task.Node.Node.ParentID,
                                Name = task.Node.Node.Name,
                                DisplayType = task.Node.Node.DisplayType,
                                OTCSPath = task.Node.Path,
                                Version = task.Node.Version,
                                Downloaded = downloadStatus.Success
                            };

                            db.InsertOrUpdateRecord(record);

                            string sanitizePathForLogs = SanitizePathForLogs(task.Node.Path, fileName);
                            var succesLog = new SuccessLogs()
                            {
                                FileID = task.Node.Node.ID.ToString(),
                                FileName = fileName,
                                FolderPath = sanitizePathForLogs,
                                LocalPath = filePath
                            };
                            csvWriterHelper.WriteRecord(succesLog, "SuccessLogs.csv");

                            string baseUrl = Settings.Default.SharePointURL.Trim('/');
                            string destPath = string.IsNullOrEmpty(baseUrl) ? "" : baseUrl + "/";
                            destPath += sanitizePathForLogs.Replace('\\', '/').Trim('/');
                            if (!destPath.EndsWith("/")) destPath += "/";
                            destPath += fileName;

                            var sGateData = new SGateData()
                            {
                                // Replace '/' with '__' in SourcePath filename for cleaner paths
                                SourcePath = filePath.Replace('/', '_') + (task.Node.Node.DisplayType == "0" || task.Node.Node.DisplayType == "751" ? "\\" : ""),
                                DestinationPath = destPath,
                                DRSCreatedBy = task.Node.Node.CreatedBy,
                                DRSCreatedDate = task.Node.Node.CreationDate.ToString("yyyy-MM-dd HH:mm:ss"),
                                DRSModifiedBy = task.Node.Node.ModifiedBy,
                                DRSModifiedDate = task.Node.Node.ModifiedDate.ToString("yyyy-MM-dd HH:mm:ss"),
                                //Version = task.Node.Version.ToString()
                            };
                            csvWriterHelper.WriteRecord(sGateData, "SGateData.csv");
                            //successLogs.Add(succesLog);

                            successfulDownloads++;

/*                            if (IsBlockedExtension(filePath))
                            {
                                filePath = await ZipFileAsync(filePath);
                            }*/

                            var isBlockedFile = IsBlockedExtension(fileName);

                            if (isBlockedFile.IsBlocked)
                            {
                                if (isInputFile)
                                {
                                    string sanitizePath = SanitizePathForLogs(task.Node.Path, fileName);
                                    var inputFileExportException = new InputFileExportException()
                                    {
                                        FileID = task.Node.Node.ID.ToString(),
                                        Status = isBlockedFile.Reason,
                                        FileName = fileName,
                                        ContentServerFolderPath = sanitizePath,
                                        DownloadedPath = filePath
                                    };
                                    csvWriterHelper.WriteRecord(inputFileExportException, "InputFile_exception.csv");
                                    //inputFileExportExceptions.Add(inputFileExportException);
                                }
                                else if (isSPOLong)
                                {
                                    string sanitizePath = SanitizePathForLogs(task.Node.Path, fileName);
                                    var longPathExportException = new LongPathExportException()
                                    {
                                        FileID = task.Node.Node.ID.ToString(),
                                        Status = isBlockedFile.Reason,
                                        FileName = fileName,
                                        ContentServerFolderPath = sanitizePath,
                                        DownloadedPath = filePath
                                    };
                                    csvWriterHelper.WriteRecord(longPathExportException, "SPOLongFilePath_exception.csv");
                                    //longPathExportExceptions.Add(longPathExportException);
                                }
                                else
                                {
                                    string sanitizePath = SanitizePathForLogs(task.Node.Path, fileName);
                                    var normalExportException = new NormalExportException()
                                    {
                                        FileID = task.Node.Node.ID.ToString(),
                                        Status = isBlockedFile.Reason,
                                        FileName = fileName,
                                        ContentServerFolderPath = sanitizePath,
                                        DownloadedPath = filePath,
                                    };
                                    csvWriterHelper.WriteRecord(normalExportException, "NormalExport_exception.csv");
                                    //normalExportExceptions.Add(normalExportException);
                                }
                            }
                            {
                                var catVal = await NodesAPI.GetCategoryValue(task.Node.Node.ID);
                                string categoriesValue = string.Join(";", catVal.Select(kv => $"{kv.Key} = {kv.Value}"));
                                if (isInputFile)
                                {
                                    string sanitizePath = SanitizePathForLogs(task.Node.Path, fileName);
                                    //var inputFileExport = new InputFileExport()
                                    //{
                                    //    FileID = task.Node.Node.ID.ToString(),
                                    //    FileName = CleanFileName(fileName),
                                    //    ContentServerFolderPath = sanitizePath,
                                    //    DownloadedPath = filePath,
                                    //    Categories = categoriesValue
                                    //};
                                    //csvWriterHelper.WriteRecord(inputFileExport, "InputFile.csv");

                                    dynamic recordToWrite = new ExpandoObject();
                                    var recordDict = (IDictionary<string, object>)recordToWrite;
                                    recordDict["FileID"] = task.Node.Node.ID.ToString();
                                    recordDict["FileName"] = CleanFileName(task.Node.Node.Name);
                                    recordDict["ContentServerFolderPath"] = sanitizePath;
                                    recordDict["DownloadedPath"] = filePath;
                                    foreach (var kv in catVal)
                                    {
                                        recordDict[kv.Key] = kv.Value;
                                    }
                                    csvWriterHelper.WriteRecord(recordToWrite, "InputFile.csv");
                                    //inputFileExports.Add(inputFileExport);
                                }
                                else if (isSPOLong)
                                {
                                    string sanitizePath = SanitizePathForLogs(task.Node.Path, fileName);
                                    //var longPathExport = new LongPathExport()
                                    //{
                                    //    FileID = task.Node.Node.ID.ToString(),
                                    //    FileName = fileName,
                                    //    ContentServerFolderPath = sanitizePath,
                                    //    DownloadedPath = filePath,
                                    //    Categories = categoriesValue
                                    //};
                                    //csvWriterHelper.WriteRecord(longPathExport, "SPOLongFilePath.csv");

                                    dynamic recordToWrite = new ExpandoObject();
                                    var recordDict = (IDictionary<string, object>)recordToWrite;
                                    recordDict["FileID"] = task.Node.Node.ID.ToString();
                                    recordDict["FileName"] = CleanFileName(task.Node.Node.Name);
                                    recordDict["ContentServerFolderPath"] = sanitizePath;
                                    recordDict["DownloadedPath"] = filePath;
                                    foreach (var kv in catVal)
                                    {
                                        recordDict[kv.Key] = kv.Value;
                                    }
                                    csvWriterHelper.WriteRecord(recordToWrite, "SPOLongFilePath.csv");
                                    //longPathExports.Add(longPathExport);
                                }
                                else
                                {
                                    string sanitizePath = SanitizePathForLogs(task.Node.Path, fileName);
                                    //var normalExport = new NormalExport()
                                    //{
                                    //    FileID = task.Node.Node.ID.ToString(),
                                    //    FileName = fileName,
                                    //    ContentServerFolderPath = sanitizePath,
                                    //    DownloadedPath = filePath,
                                    //    Categories = categoriesValue
                                    //};
                                    //csvWriterHelper.WriteRecord(normalExport, "NormalExport.csv");

                                    dynamic recordToWrite = new ExpandoObject();
                                    var recordDict = (IDictionary<string, object>)recordToWrite;
                                    recordDict["FileID"] = task.Node.Node.ID.ToString();
                                    recordDict["FileName"] = CleanFileName(task.Node.Node.Name);
                                    recordDict["ContentServerFolderPath"] = sanitizePath;
                                    recordDict["DownloadedPath"] = filePath;
                                    foreach (var kv in catVal)
                                    {
                                        recordDict[kv.Key] = kv.Value;
                                    }
                                    csvWriterHelper.WriteRecord(recordToWrite, "NormalExport.csv");
                                    //normalExports.Add(normalExport);
                                }
                            }
                        }
                        else
                        {
                            var record = new DownloadRecord
                            {
                                Id = task.Node.Node.ID,
                                ParentId = task.Node.Node.ParentID,
                                Name = fileName,
                                DisplayType = task.Node.Node.DisplayType,
                                OTCSPath = task.Node.Path,
                                Version = task.Node.Version,
                                Downloaded = downloadStatus.Success
                            };

                            db.InsertOrUpdateRecord(record);

                            string sanitizePathForLogs = SanitizePathForLogs(task.Node.Path, fileName);
                            var failedLog = new FailedLogs()
                            {
                                FileID = task.Node.Node.ID.ToString(),
                                FileName = fileName,
                                FolderPath = sanitizePathForLogs,
                                ErrorMessage = downloadStatus.ErrorMessage
                            };
                            csvWriterHelper.WriteRecord(failedLog, "FailedLogs.csv");
                            //failedLogs.Add(failedLog);
                            failedDownloads++;
                        }
                        totalItemsProcessed++;

                    }
                    catch (Exception ex)
                    {
                        string errMsg = $"Error processing {task.Node.Node.Name}: {ex.Message}";
                        Logger.Error(errMsg);
                        LogErrorToFile(faillog, task.Node, ex, startTime);

                        string sanitizePathForLogs = SanitizePathForLogs(task.Node.Path);
                        var failedLog = new FailedLogs()
                        {
                            FileID = task.Node.Node.ID.ToString(),
                            FileName = task.Node.Node.Name,
                            FolderPath = sanitizePathForLogs,
                            ErrorMessage = errMsg
                        };
                        csvWriterHelper.WriteRecord(failedLog, "FailedLogs.csv");
                        //failedLogs.Add(failedLog);
                        failedDownloads++;
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }));
            }

            await Task.WhenAll(tasks);

            /*
            Write to CSV
            if (normalExports.Count > 0)
            {
                string baseReportPath = Properties.Settings.Default.CsvOutput;
                string csvPath = Path.Combine(baseReportPath, "NormalExport.csv");
                using (var writer = new StreamWriter(csvPath))
                using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    csv.WriteRecords(normalExports);
                }
            }
            if (normalExportExceptions.Count > 0)
            {
                string baseReportPath = Properties.Settings.Default.CsvOutput;
                string csvPath = Path.Combine(baseReportPath, "NormalExport_exception.csv");
                using (var writer = new StreamWriter(csvPath))
                using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    csv.WriteRecords(normalExportExceptions);
                }
            }
            if (inputFileExports.Count > 0)
            {
                string baseReportPath = Properties.Settings.Default.CsvOutput;
                string csvPath = Path.Combine(baseReportPath, "InputFile.csv");
                using (var writer = new StreamWriter(csvPath))
                using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    csv.WriteRecords(inputFileExports);
                }
            }
            if (inputFileExportExceptions.Count > 0)
            {
                string baseReportPath = Properties.Settings.Default.CsvOutput;
                string csvPath = Path.Combine(baseReportPath, "InputFile_exception.csv");
                using (var writer = new StreamWriter(csvPath))
                using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    csv.WriteRecords(inputFileExportExceptions);
                }
            }
            if (longPathExports.Count > 0)
            {
                string baseReportPath = Properties.Settings.Default.CsvOutput;
                string csvPath = Path.Combine(baseReportPath, "SPOLongFilePath.csv");
                using (var writer = new StreamWriter(csvPath))
                using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    csv.WriteRecords(longPathExports);
                }
            }
            if (longPathExportExceptions.Count > 0)
            {
                string baseReportPath = Properties.Settings.Default.CsvOutput;
                string csvPath = Path.Combine(baseReportPath, "SPOLongFilePath_exception.csv");
                using (var writer = new StreamWriter(csvPath))
                using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    csv.WriteRecords(longPathExportExceptions);
                }
            }
            if (successLogs.Count > 0)
            {
                string baseReportPath = Properties.Settings.Default.CsvOutput;
                string csvPath = Path.Combine(baseReportPath, "SuccessLogs.csv");
                using (var writer = new StreamWriter(csvPath))
                using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    csv.WriteRecords(successLogs);
                }
            }
            if (failedLogs.Count > 0)
            {
                string baseReportPath = Properties.Settings.Default.CsvOutput;
                string csvPath = Path.Combine(baseReportPath, "FailedLogs.csv");
                using (var writer = new StreamWriter(csvPath))
                using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    csv.WriteRecords(failedLogs);
                }
            }
            */
            Logger.Info("Download process completed.");
            Logger.Info($"Total items processed: {totalItemsProcessed}");
            Logger.Info($"Successful downloads: {successfulDownloads}");
            Logger.Info($"Failed downloads: {failedDownloads}");
        }

        private static async Task ProcessDownloadsAsyncStructureLastRecord(List<CustomNode> downloadFolders, string baseFilePath, string token,
    DownloadDatabase db, DateTime tokenTime, string userName, string password, string faillog, bool isResume, DateTime startTime)
        {
            Logger.Info($"Using last record logic for resume feature");
            CsvWriterHelper csvWriterHelper = new CsvWriterHelper(Properties.Settings.Default.CsvOutput);
            int totalItemsProcessed = 0;
            int successfulDownloads = 0;
            int failedDownloads = 0;
            var newFormats = new List<NewFormat>();

            /*
            // Export Class
            var normalExports = new List<NormalExport>();
            var normalExportExceptions = new List<NormalExportException>();
            var inputFileExports = new List<InputFileExport>();
            var inputFileExportExceptions = new List<InputFileExportException>();
            var longPathExports = new List<LongPathExport>();
            var longPathExportExceptions = new List<LongPathExportException>();

            // Log Class
            var successLogs = new List<SuccessLogs>();
            var failedLogs = new List<FailedLogs>();*/

            var httpClient = new HttpClient();
            const int timeoutSeconds = 900; // 15 minutes timeout
            httpClient.Timeout = TimeSpan.FromSeconds(timeoutSeconds);

            var downloadQueue = new BlockingCollection<DownloadTask>();

            // Read the max thread count from config
            int maxThreads = Properties.Settings.Default.MaxThreads;
            if (maxThreads == 0)
            {
                maxThreads = Environment.ProcessorCount; // Default to number of processors
            }
            // Use SemaphoreSlim with configurable thread count
            var semaphore = new SemaphoreSlim(maxThreads);

            long lastDownloadedFolderId = isResume ? db.GetLastDownloadedFolderId() : 0;

            var pathHandler = new PathLengthHandler(baseFilePath);

            var longFolderPath = Properties.Settings.Default.DestPath;

            // First pass: Create only actual folder structures
            foreach (var df in downloadFolders.Where(x => (x.Node.DisplayType == "0" || x.Node.DisplayType == "141") && x.Node.ID > lastDownloadedFolderId))
            {

                totalItemsProcessed++;
                try
                {
                    var (actualPath, isInputFolder, isSPOLong) = pathHandler.ProcessFolderPath(df);
                    if (!Directory.Exists(actualPath))
                    {
                        var isBlockedStatus = IsBlockedFolder(df.Node.Name);
                        if (isBlockedStatus.IsBlocked)
                        {
                            if (actualPath.Length > Properties.Settings.Default.PathMinLength)
                            {
                                Logger.Info($"Skipped directory creation due to path length exceeding limit: {actualPath}");
                            }
                            else
                            {
                                Directory.CreateDirectory(actualPath);
                                Logger.Info($"Created directory: {actualPath}");
                            }
                            string sanitizePath = df.Path;
                            if (isInputFolder)
                            {
                                var inputFileExportException = new InputFileExportException()
                                {
                                    FileID = df.Node.ID.ToString(),
                                    Status = isBlockedStatus.Reason,
                                    FileName = df.Node.Name,
                                    ContentServerFolderPath = sanitizePath,
                                    DownloadedPath = actualPath
                                };
                                csvWriterHelper.WriteRecord(inputFileExportException, "InputFile_exception.csv");
                                //inputFileExportExceptions.Add(inputFileExportException);
                            }
                            else if (isSPOLong)
                            {
                                var longPathExportException = new LongPathExportException()
                                {
                                    FileID = df.Node.ID.ToString(),
                                    Status = isBlockedStatus.Reason,
                                    FileName = df.Node.Name,
                                    ContentServerFolderPath = sanitizePath,
                                    DownloadedPath = actualPath
                                };
                                csvWriterHelper.WriteRecord(longPathExportException, "SPOLongFilePath_exception.csv");
                                //longPathExportExceptions.Add(longPathExportException);
                            }
                            else
                            {
                                var normalExportException = new NormalExportException()
                                {
                                    FileID = df.Node.ID.ToString(),
                                    Status = isBlockedStatus.Reason,
                                    FileName = df.Node.Name,
                                    ContentServerFolderPath = sanitizePath,
                                    DownloadedPath = actualPath
                                };
                                csvWriterHelper.WriteRecord(normalExportException, "NormalExport_exception.csv");
                                //normalExportExceptions.Add(normalExportException);
                            }

                            var succesLog = new SuccessLogs()
                            {
                                FileID = df.Node.ID.ToString(),
                                FileName = df.Node.Name,
                                FolderPath = sanitizePath,
                                LocalPath = actualPath
                            };
                            // Add status for blocked folder here
                        }
                        else
                        {
                            if (actualPath.Length > Properties.Settings.Default.PathMinLength)
                            {
                                Logger.Info($"Skipped directory creation due to path length exceeding limit: {actualPath}");
                            }
                            else
                            {
                                Directory.CreateDirectory(actualPath);
                                Logger.Info($"Created directory: {actualPath}");
                            }
                            var catVal = await NodesAPI.GetCategoryValue(df.Node.ID);
                            string categoriesValue = string.Join(";", catVal.Select(kv => $"{kv.Key} = {kv.Value}"));
                            string sanitizePath = df.Path;
                            if (isInputFolder)
                            {
                                //var inputFileExport = new InputFileExport()
                                //{
                                //    FileID = df.Node.ID.ToString(),
                                //    FileName = CleanFileName(df.Node.Name),
                                //    ContentServerFolderPath = sanitizePath,
                                //    DownloadedPath = actualPath,
                                //    Categories = categoriesValue
                                //};
                                //csvWriterHelper.WriteRecord(inputFileExport, "InputFile.csv");

                                dynamic recordToWrite = new ExpandoObject();
                                var recordDict = (IDictionary<string, object>)recordToWrite;
                                recordDict["FileID"] = df.Node.ID.ToString();
                                recordDict["FileName"] = CleanFileName(df.Node.Name);
                                recordDict["ContentServerFolderPath"] = sanitizePath;
                                recordDict["DownloadedPath"] = actualPath;
                                foreach (var kv in catVal)
                                {
                                    recordDict[kv.Key] = kv.Value;
                                }
                                csvWriterHelper.WriteRecord(recordToWrite, "InputFile.csv");
                                //inputFileExports.Add(inputFileExport);
                            }
                            else if (isSPOLong)
                            {
                                //var longPathExport = new LongPathExport()
                                //{
                                //    FileID = df.Node.ID.ToString(),
                                //    FileName = df.Node.Name,
                                //    ContentServerFolderPath = sanitizePath,
                                //    DownloadedPath = actualPath,
                                //    Categories = categoriesValue
                                //};
                                //csvWriterHelper.WriteRecord(longPathExport, "SPOLongFilePath.csv");

                                dynamic recordToWrite = new ExpandoObject();
                                var recordDict = (IDictionary<string, object>)recordToWrite;
                                recordDict["FileID"] = df.Node.ID.ToString();
                                recordDict["FileName"] = CleanFileName(df.Node.Name);
                                recordDict["ContentServerFolderPath"] = sanitizePath;
                                recordDict["DownloadedPath"] = actualPath;
                                foreach (var kv in catVal)
                                {
                                    recordDict[kv.Key] = kv.Value;
                                }
                                csvWriterHelper.WriteRecord(recordToWrite, "SPOLongFilePath.csv");
                                //longPathExports.Add(longPathExport);
                            }
                            else
                            {
                                //var normalExport = new NormalExport()
                                //{
                                //    FileID = df.Node.ID.ToString(),
                                //    FileName = df.Node.Name,
                                //    ContentServerFolderPath = sanitizePath,
                                //    DownloadedPath = actualPath,
                                //    Categories = categoriesValue
                                //};
                                //csvWriterHelper.WriteRecord(normalExport, "NormalExport.csv");

                                dynamic recordToWrite = new ExpandoObject();
                                var recordDict = (IDictionary<string, object>)recordToWrite;
                                recordDict["FileID"] = df.Node.ID.ToString();
                                recordDict["FileName"] = CleanFileName(df.Node.Name);
                                recordDict["ContentServerFolderPath"] = sanitizePath;
                                recordDict["DownloadedPath"] = actualPath;
                                foreach (var kv in catVal)
                                {
                                    recordDict[kv.Key] = kv.Value;
                                }
                                csvWriterHelper.WriteRecord(recordToWrite, "NormalExport.csv");
                                //normalExports.Add(normalExport);
                            }
                                
                            var succesLog = new SuccessLogs()
                            {
                                FileID = df.Node.ID.ToString(),
                                FileName = df.Node.Name,
                                FolderPath = sanitizePath,
                                LocalPath = actualPath
                            };
                            csvWriterHelper.WriteRecord(succesLog, "SuccessLogs.csv");

                            string baseUrl = Settings.Default.SharePointURL.Trim('/');
                            string destPath = string.IsNullOrEmpty(baseUrl) ? "" : baseUrl + "/";
                            destPath += sanitizePath.Replace('\\', '/').Trim('/');
                            if (!destPath.EndsWith("/")) destPath += "/";
                            destPath += CleanFileName(df.Node.Name);
                            if (df.Node.DisplayType == "0" || df.Node.DisplayType == "751") destPath += "/";

                            var sGateData = new SGateData()
                            {
                                SourcePath = actualPath + (df.Node.DisplayType == "0" || df.Node.DisplayType == "751" ? "\\" : ""),
                                DestinationPath = destPath,
                                DRSCreatedBy = df.Node.CreatedBy,
                                DRSCreatedDate = df.Node.CreationDate.ToString("yyyy-MM-dd HH:mm:ss"),
                                DRSModifiedBy = df.Node.ModifiedBy,
                                DRSModifiedDate = df.Node.ModifiedDate.ToString("yyyy-MM-dd HH:mm:ss"),
                                //Version = df.Version.ToString()
                            };
                            csvWriterHelper.WriteRecord(sGateData, "SGateData.csv");
                            //successLogs.Add(succesLog);
                        }
                    }

                    var record = new DownloadRecord
                    {
                        Id = df.Node.ID,
                        ParentId = df.Node.ParentID,
                        Name = df.Node.Name,
                        DisplayType = df.Node.DisplayType,
                        OTCSPath = df.Path,
                        Version = df.Version,
                        Downloaded = true // Mark folders as downloaded
                    };
                    db.InsertOrUpdateRecord(record);
                }
                catch (Exception ex)
                {
                    string errMSg = $"Error creating directory structure for {df.Path}: {ex.Message}";
                    Logger.Error(errMSg);
                    Logger.Error(ex.StackTrace);
                    string sanitizePath = df.Path;
                    var failedLog = new FailedLogs()
                    {
                        FileID = df.Node.ID.ToString(),
                        FileName = df.Node.Name,
                        FolderPath = sanitizePath,
                        ErrorMessage = errMSg
                    };
                    csvWriterHelper.WriteRecord(failedLog, "FailedLogs.csv");
                    //failedLogs.Add(failedLog);
                }
            }

            // Populate the download queue with files only
            long lastDownloadedDocumentId = isResume ? db.GetLastDownloadedDocumentId() : 0;
            foreach (var df in downloadFolders)
            {
                if ((df.Node.DisplayType == "144" || df.Node.DisplayType == "749") && df.Node.ID > lastDownloadedDocumentId)
                {
                    
                   downloadQueue.Add(new DownloadTask { Node = df });
                    
                }
            }

            // Mark the queue as complete for adding
            downloadQueue.CompleteAdding();

            // Process the download queue
            var tasks = new List<Task>();

            // Get prefix for sharepoint destination
            string sharePointDest = Properties.Settings.Default.DestPath;

            foreach (var task in downloadQueue.GetConsumingEnumerable())
            {
                await semaphore.WaitAsync();
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        // For files, we only create the parent folder structure
                        string[] pathParts = task.Node.Path.Split(new[] { '\\' }, StringSplitOptions.RemoveEmptyEntries);
                        string targetDirectory = GetParentFolderPath(pathParts, baseFilePath, db);

                        string fileName = CleanFileName(task.Node.Node.Name);
                        if (string.IsNullOrEmpty(Path.GetExtension(fileName)))
                        {
                            if (!string.IsNullOrEmpty(task.Node.MimeType))
                            {
                                string extension;

                                // Check for special MIME types first
                                switch (task.Node.MimeType.ToLower())
                                {
                                    case "application/vnd.ms-outlook":
                                    case "application/x-msmsg":
                                    case "application/x-outlook-msg":
                                        extension = ".msg";
                                        break;
                                    case "message/rfc822":
                                        extension = ".eml";
                                        break;
                                    default:
                                        // Fall back to MimeTypeMap for standard MIME types
                                        extension = MimeTypeMap.GetExtension(task.Node.MimeType);
                                        break;
                                }

                                // Use the extension
                                if (!string.IsNullOrEmpty(extension))
                                {
                                    fileName += extension;
                                }
                            }
                        }
                        string filePath = Path.Combine(targetDirectory, fileName);

                        var (processedFilePath, isInputFile, isSPOLong) = pathHandler.ProcessFilePath(filePath, task.Node);

                        var uniqueRes = GetUniqueFilePath(processedFilePath);

                        filePath = uniqueRes.filePath;
                        if (uniqueRes.isDuplicate)
                        {
/*                            if (!db.DoesLocalPathExist(processedFilePath))
                            {
                                var record = new DownloadRecord
                                {
                                    Id = task.Node.Node.ID,
                                    ParentId = task.Node.Node.ParentID,
                                    Name = task.Node.Node.Name,
                                    DisplayType = task.Node.Node.DisplayType,
                                    OTCSPath = task.Node.Path,
                                    LocalPath = processedFilePath,
                                    Version = task.Node.Version,
                                    Downloaded = true
                                };

                                db.InsertOrUpdateRecord(record);

                                string sanitizePathForLogs = task.Node.Path.Replace(">", "/");
                                var succesLog = new SuccessLogs()
                                {
                                    FileID = task.Node.Node.ID.ToString(),
                                    FileName = task.Node.Node.Name,
                                    FolderPath = sanitizePathForLogs,
                                    LocalPath = processedFilePath
                                };
                                csvWriterHelper.WriteRecord(succesLog, "SuccessLogs.csv");
                                //successLogs.Add(succesLog);

                                successfulDownloads++;

                                var isBlockedFile = IsBlockedExtension(fileName);

                                if (isBlockedFile.IsBlocked)
                                {
                                    if (isInputFile)
                                    {
                                        string sanitizePath = task.Node.Path.Replace(">", "/");
                                        var inputFileExportException = new InputFileExportException()
                                        {
                                            FileID = task.Node.Node.ID.ToString(),
                                            Status = isBlockedFile.Reason,
                                            FileName = task.Node.Node.Name,
                                            ContentServerFolderPath = sanitizePath,
                                            DownloadedPath = processedFilePath
                                        };
                                        csvWriterHelper.WriteRecord(inputFileExportException, "InputFile_exception.csv");
                                        //inputFileExportExceptions.Add(inputFileExportException);
                                    }
                                    else if (isSPOLong)
                                    {
                                        string sanitizePath = task.Node.Path.Replace(">", "/");
                                        var longPathExportException = new LongPathExportException()
                                        {
                                            FileID = task.Node.Node.ID.ToString(),
                                            Status = isBlockedFile.Reason,
                                            FileName = task.Node.Node.Name,
                                            ContentServerFolderPath = sanitizePath,
                                            DownloadedPath = processedFilePath
                                        };
                                        csvWriterHelper.WriteRecord(longPathExportException, "SPOLongFilePath_exception.csv");
                                        //longPathExportExceptions.Add(longPathExportException);
                                    }
                                    else
                                    {
                                        string sanitizePath = task.Node.Path.Replace(">", "/");
                                        var normalExportException = new NormalExportException()
                                        {
                                            FileID = task.Node.Node.ID.ToString(),
                                            Status = isBlockedFile.Reason,
                                            FileName = task.Node.Node.Name,
                                            ContentServerFolderPath = sanitizePath,
                                            DownloadedPath = processedFilePath,
                                        };
                                        csvWriterHelper.WriteRecord(normalExportException, "NormalExport_exception.csv");
                                        //normalExportExceptions.Add(normalExportException);
                                    }
                                }
                                {
                                    if (isInputFile)
                                    {
                                        string sanitizePath = task.Node.Path.Replace(">", "/");
                                        var inputFileExport = new InputFileExport()
                                        {
                                            FileID = task.Node.Node.ID.ToString(),
                                            FileName = task.Node.Node.Name,
                                            ContentServerFolderPath = sanitizePath,
                                            DownloadedPath = processedFilePath
                                        };
                                        csvWriterHelper.WriteRecord(inputFileExport, "InputFile.csv");
                                        //inputFileExports.Add(inputFileExport);
                                    }
                                    else if (isSPOLong)
                                    {
                                        string sanitizePath = task.Node.Path.Replace(">", "/");
                                        var longPathExport = new LongPathExport()
                                        {
                                            FileID = task.Node.Node.ID.ToString(),
                                            FileName = task.Node.Node.Name,
                                            ContentServerFolderPath = sanitizePath,
                                            DownloadedPath = processedFilePath
                                        };
                                        csvWriterHelper.WriteRecord(longPathExport, "SPOLongFilePath.csv");
                                        //longPathExports.Add(longPathExport);
                                    }
                                    else
                                    {
                                        string sanitizePath = task.Node.Path.Replace(">", "/");
                                        var normalExport = new NormalExport()
                                        {
                                            FileID = task.Node.Node.ID.ToString(),
                                            FileName = task.Node.Node.Name,
                                            ContentServerFolderPath = sanitizePath,
                                            DownloadedPath = processedFilePath,
                                        };
                                        csvWriterHelper.WriteRecord(normalExport, "NormalExport.csv");
                                        //normalExports.Add(normalExport);
                                    }
                                }

                            }*/
                            task.Node.Node.Name = Path.GetFileName(uniqueRes.filePath);
                        }
                        Logger.Info("check the processed pat of a file in program: " + filePath);
                        if (!Directory.Exists(Path.GetDirectoryName(filePath)))
                        {
                            Logger.Info("check file path exist: " + Path.GetDirectoryName(filePath));
                            Directory.CreateDirectory(Path.GetDirectoryName(filePath));
                        }
                        var downloadStatus = await DownloadNodeAsync(task.Node, token, filePath, httpClient);
                        if (downloadStatus.Success)
                        {
                            var record = new DownloadRecord
                            {
                                Id = task.Node.Node.ID,
                                ParentId = task.Node.Node.ParentID,
                                Name = task.Node.Node.Name,
                                DisplayType = task.Node.Node.DisplayType,
                                OTCSPath = task.Node.Path,
                                Version = task.Node.Version,
                                Downloaded = downloadStatus.Success
                            };

                            db.InsertOrUpdateRecord(record);

                            string sanitizePathForLogs = Path.GetDirectoryName(task.Node.Path);
                            var succesLog = new SuccessLogs()
                            {
                                FileID = task.Node.Node.ID.ToString(),
                                FileName = task.Node.Node.Name,
                                FolderPath = sanitizePathForLogs,
                                LocalPath = filePath
                            };
                            csvWriterHelper.WriteRecord(succesLog, "SuccessLogs.csv");

                            string baseUrl = Settings.Default.SharePointURL.Trim('/');
                            string destPath = string.IsNullOrEmpty(baseUrl) ? "" : baseUrl + "/";
                            destPath += sanitizePathForLogs.Replace('\\', '/').Trim('/');
                            if (!destPath.EndsWith("/")) destPath += "/";
                            destPath += fileName;

                            var sGateData = new SGateData()
                            {
                                // Replace '/' with '__' in SourcePath filename for cleaner paths
                                SourcePath = filePath.Replace('/', '_') + (task.Node.Node.DisplayType == "0" || task.Node.Node.DisplayType == "751" ? "\\" : ""),
                                DestinationPath = destPath,
                                DRSCreatedBy = task.Node.Node.CreatedBy,
                                DRSCreatedDate = task.Node.Node.CreationDate.ToString("yyyy-MM-dd HH:mm:ss"),
                                DRSModifiedBy = task.Node.Node.ModifiedBy,
                                DRSModifiedDate = task.Node.Node.ModifiedDate.ToString("yyyy-MM-dd HH:mm:ss"),
                                //Version = task.Node.Version.ToString()
                            };
                            csvWriterHelper.WriteRecord(sGateData, "SGateData.csv");
                            //successLogs.Add(succesLog);

                            successfulDownloads++;

/*                            if (IsBlockedExtension(filePath))
                            {
                                filePath = await ZipFileAsync(filePath);
                            }*/

                            var isBlockedFile = IsBlockedExtension(fileName);

                            if (isBlockedFile.IsBlocked)
                            {
                                if (isInputFile)
                                {
                                    string sanitizePath = task.Node.Path;
                                    var inputFileExportException = new InputFileExportException()
                                    {
                                        FileID = task.Node.Node.ID.ToString(),
                                        Status = isBlockedFile.Reason,
                                        FileName = task.Node.Node.Name,
                                        ContentServerFolderPath = sanitizePath,
                                        DownloadedPath = filePath
                                    };
                                    csvWriterHelper.WriteRecord(inputFileExportException, "InputFile_exception.csv");
                                    //inputFileExportExceptions.Add(inputFileExportException);
                                }
                                else if (isSPOLong)
                                {
                                    string sanitizePath = task.Node.Path;
                                    var longPathExportException = new LongPathExportException()
                                    {
                                        FileID = task.Node.Node.ID.ToString(),
                                        Status = isBlockedFile.Reason,
                                        FileName = task.Node.Node.Name,
                                        ContentServerFolderPath = sanitizePath,
                                        DownloadedPath = filePath
                                    };
                                    csvWriterHelper.WriteRecord(longPathExportException, "SPOLongFilePath_exception.csv");
                                    //longPathExportExceptions.Add(longPathExportException);
                                }
                                else
                                {
                                    string sanitizePath = task.Node.Path;
                                    var normalExportException = new NormalExportException()
                                    {
                                        FileID = task.Node.Node.ID.ToString(),
                                        Status = isBlockedFile.Reason,
                                        FileName = task.Node.Node.Name,
                                        ContentServerFolderPath = sanitizePath,
                                        DownloadedPath = filePath,
                                    };
                                    csvWriterHelper.WriteRecord(normalExportException, "NormalExport_exception.csv");
                                    //normalExportExceptions.Add(normalExportException);
                                }
                            }
                            {
                                var catVal = await NodesAPI.GetCategoryValue(task.Node.Node.ID);
                                string categoriesValue = string.Join(";", catVal.Select(kv => $"{kv.Key} = {kv.Value}"));
                                if (isInputFile)
                                {
                                    string sanitizePath = task.Node.Path;
                                    //var inputFileExport = new InputFileExport()
                                    //{
                                    //    FileID = task.Node.Node.ID.ToString(),
                                    //    FileName = CleanFileName(task.Node.Node.Name),
                                    //    ContentServerFolderPath = sanitizePath,
                                    //    DownloadedPath = filePath,
                                    //    Categories = categoriesValue
                                    //};
                                    //csvWriterHelper.WriteRecord(inputFileExport, "InputFile.csv");

                                    dynamic recordToWrite = new ExpandoObject();
                                    var recordDict = (IDictionary<string, object>)recordToWrite;
                                    recordDict["FileID"] = task.Node.Node.ID.ToString();
                                    recordDict["FileName"] = CleanFileName(task.Node.Node.Name);
                                    recordDict["ContentServerFolderPath"] = sanitizePath;
                                    recordDict["DownloadedPath"] = filePath;
                                    foreach (var kv in catVal)
                                    {
                                        recordDict[kv.Key] = kv.Value;
                                    }
                                    csvWriterHelper.WriteRecord(recordToWrite, "InputFile.csv");
                                    //inputFileExports.Add(inputFileExport);
                                }
                                else if (isSPOLong)
                                {
                                    string sanitizePath = task.Node.Path;
                                    //var longPathExport = new LongPathExport()
                                    //{
                                    //    FileID = task.Node.Node.ID.ToString(),
                                    //    FileName = task.Node.Node.Name,
                                    //    ContentServerFolderPath = sanitizePath,
                                    //    DownloadedPath = filePath,
                                    //    Categories = categoriesValue
                                    //};
                                    //csvWriterHelper.WriteRecord(longPathExport, "SPOLongFilePath.csv");

                                    dynamic recordToWrite = new ExpandoObject();
                                    var recordDict = (IDictionary<string, object>)recordToWrite;
                                    recordDict["FileID"] = task.Node.Node.ID.ToString();
                                    recordDict["FileName"] = CleanFileName(task.Node.Node.Name);
                                    recordDict["ContentServerFolderPath"] = sanitizePath;
                                    recordDict["DownloadedPath"] = filePath;
                                    foreach (var kv in catVal)
                                    {
                                        recordDict[kv.Key] = kv.Value;
                                    }
                                    csvWriterHelper.WriteRecord(recordToWrite, "SPOLongFilePath.csv");
                                    //longPathExports.Add(longPathExport);
                                }
                                else
                                {
                                    string sanitizePath = task.Node.Path;
                                    //var normalExport = new NormalExport()
                                    //{
                                    //    FileID = task.Node.Node.ID.ToString(),
                                    //    FileName = task.Node.Node.Name,
                                    //    ContentServerFolderPath = sanitizePath,
                                    //    DownloadedPath = filePath,
                                    //    Categories = categoriesValue
                                    //};
                                    //csvWriterHelper.WriteRecord(normalExport, "NormalExport.csv");

                                    dynamic recordToWrite = new ExpandoObject();
                                    var recordDict = (IDictionary<string, object>)recordToWrite;
                                    recordDict["FileID"] = task.Node.Node.ID.ToString();
                                    recordDict["FileName"] = CleanFileName(task.Node.Node.Name);
                                    recordDict["ContentServerFolderPath"] = sanitizePath;
                                    recordDict["DownloadedPath"] = filePath;
                                    foreach (var kv in catVal)
                                    {
                                        recordDict[kv.Key] = kv.Value;
                                    }
                                    csvWriterHelper.WriteRecord(recordToWrite, "NormalExport.csv");
                                    //normalExports.Add(normalExport);
                                }
                            }
                        }
                        else
                        {
                            var record = new DownloadRecord
                            {
                                Id = task.Node.Node.ID,
                                ParentId = task.Node.Node.ParentID,
                                Name = task.Node.Node.Name,
                                DisplayType = task.Node.Node.DisplayType,
                                OTCSPath = task.Node.Path,
                                Version = task.Node.Version,
                                Downloaded = downloadStatus.Success
                            };

                            db.InsertOrUpdateRecord(record);

                            string sanitizePathForLogs = task.Node.Path;
                            var failedLog = new FailedLogs()
                            {
                                FileID = task.Node.Node.ID.ToString(),
                                FileName = task.Node.Node.Name,
                                FolderPath = sanitizePathForLogs,
                                ErrorMessage = downloadStatus.ErrorMessage
                            };
                            csvWriterHelper.WriteRecord(failedLog, "FailedLogs.csv");
                            //failedLogs.Add(failedLog);
                            failedDownloads++;
                        }
                        totalItemsProcessed++;

                    }
                    catch (Exception ex)
                    {
                        string errMsg = $"Error processing {task.Node.Node.Name}: {ex.Message}";
                        Logger.Error(errMsg);
                        LogErrorToFile(faillog, task.Node, ex, startTime);

                        string sanitizePathForLogs = task.Node.Path;
                        var failedLog = new FailedLogs()
                        {
                            FileID = task.Node.Node.ID.ToString(),
                            FileName = task.Node.Node.Name,
                            FolderPath = sanitizePathForLogs,
                            ErrorMessage = errMsg
                        };
                        csvWriterHelper.WriteRecord(failedLog, "FailedLogs.csv");
                        //failedLogs.Add(failedLog);
                        failedDownloads++;
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }));
            }

            await Task.WhenAll(tasks);
            /*
                        // Write to CSV
                        if (normalExports.Count > 0)
                        {
                            string baseReportPath = Properties.Settings.Default.CsvOutput;
                            string csvPath = Path.Combine(baseReportPath, "NormalExport.csv");
                            using (var writer = new StreamWriter(csvPath))
                            using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                            {
                                csv.WriteRecords(normalExports);
                            }
                        }
                        if (normalExportExceptions.Count > 0)
                        {
                            string baseReportPath = Properties.Settings.Default.CsvOutput;
                            string csvPath = Path.Combine(baseReportPath, "NormalExport_exception.csv");
                            using (var writer = new StreamWriter(csvPath))
                            using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                            {
                                csv.WriteRecords(normalExportExceptions);
                            }
                        }
                        if (inputFileExports.Count > 0)
                        {
                            string baseReportPath = Properties.Settings.Default.CsvOutput;
                            string csvPath = Path.Combine(baseReportPath, "InputFile.csv");
                            using (var writer = new StreamWriter(csvPath))
                            using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                            {
                                csv.WriteRecords(inputFileExports);
                            }
                        }
                        if (inputFileExportExceptions.Count > 0)
                        {
                            string baseReportPath = Properties.Settings.Default.CsvOutput;
                            string csvPath = Path.Combine(baseReportPath, "InputFile_exception.csv");
                            using (var writer = new StreamWriter(csvPath))
                            using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                            {
                                csv.WriteRecords(inputFileExportExceptions);
                            }
                        }
                        if (longPathExports.Count > 0)
                        {
                            string baseReportPath = Properties.Settings.Default.CsvOutput;
                            string csvPath = Path.Combine(baseReportPath, "SPOLongFilePath.csv");
                            using (var writer = new StreamWriter(csvPath))
                            using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                            {
                                csv.WriteRecords(longPathExports);
                            }
                        }
                        if (longPathExportExceptions.Count > 0)
                        {
                            string baseReportPath = Properties.Settings.Default.CsvOutput;
                            string csvPath = Path.Combine(baseReportPath, "SPOLongFilePath_exception.csv");
                            using (var writer = new StreamWriter(csvPath))
                            using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                            {
                                csv.WriteRecords(longPathExportExceptions);
                            }
                        }
                        if (successLogs.Count > 0)
                        {
                            string baseReportPath = Properties.Settings.Default.CsvOutput;
                            string csvPath = Path.Combine(baseReportPath, "SuccessLogs.csv");
                            using (var writer = new StreamWriter(csvPath))
                            using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                            {
                                csv.WriteRecords(successLogs);
                            }
                        }
                        if (failedLogs.Count > 0)
                        {
                            string baseReportPath = Properties.Settings.Default.CsvOutput;
                            string csvPath = Path.Combine(baseReportPath, "FailedLogs.csv");
                            using (var writer = new StreamWriter(csvPath))
                            using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                            {
                                csv.WriteRecords(failedLogs);
                            }
                        }*/

            Logger.Info("Download process completed.");
            Logger.Info($"Total items processed: {totalItemsProcessed}");
            Logger.Info($"Successful downloads: {successfulDownloads}");
            Logger.Info($"Failed downloads: {failedDownloads}");
        }

        private static string GetSanitizedPath(string nodePath)
        {
            string sanitizedPath = nodePath.Replace(">", "/");
            string[] segments = sanitizedPath.Split('\\');
            string[] pathSegments = segments.Take(segments.Length - 1).ToArray();
            return Properties.Settings.Default.DestPath + string.Join("\\", pathSegments);
        }

        public static List<CustomNode> FilterOutExistingDownloads(List<CustomNode> downloadFolders, DownloadDatabase db)
        {
            // Get all existing IDs from database in a single query for better performance
            var existingIds = db.GetAllDownloadedIds();

            // Convert to HashSet for O(1) lookup performance
            var existingIdSet = new HashSet<long>(existingIds);

            // Filter out nodes that already exist in database
            return downloadFolders.Where(df => !existingIdSet.Contains(df.Node.ID)).ToList();
        }
        private static string GetLongFolderPath(string folderPath, long nodeID)
        {
            string directory = Properties.Settings.Default.LongFilePathLocation;
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }
            return Path.Combine(directory, nodeID.ToString());
        }

        private static string GetParentFolderPath(string[] pathParts, string baseFilePath, DownloadDatabase db)
        {
            string targetDirectory = baseFilePath;

            // The last segment in pathParts is the document name (which may contain '/' or '\')
            // We only want to process folder segments, so exclude the last one
            int folderSegmentCount = pathParts.Length > 0 ? pathParts.Length - 1 : 0;

            // Build the full potential path to check length
            string fullPath = baseFilePath;
            for (int i = 0; i < folderSegmentCount; i++)
            {
                // Use CleanFilePath for folder segments (not CleanFileName, which is for document names)
                // Folder segments should not contain path separators, but if they do, CleanFilePath will handle them
                fullPath = Path.Combine(fullPath, CleanFilePath(pathParts[i]));
            }
            // Use original path construction
            for (int i = 0; i < folderSegmentCount; i++)
            {
                // Use CleanFilePath for folder segments (not CleanFileName, which is for document names)
                targetDirectory = Path.Combine(targetDirectory, CleanFilePath(pathParts[i]));
            }

            return targetDirectory;
        }
        public static string CleanFilePath(string filePath)
        {
            // Pattern includes common illegal path characters
            // Note: We keep forward slash / and backslash \ since they are path separators
            string pattern = @"[~#%&*{}<>?\|\""']";
            Regex regex = new Regex(pattern);

            // Replace illegal characters with underscore
            return regex.Replace(filePath, "_");
        }
        static string CleanFileName(string fileName)
        {
            if (string.IsNullOrWhiteSpace(fileName))
                return string.Empty;

            // If the name contains path separators (like "Email Demo/\_1811..."), 
            // replace them with underscores to treat the whole string as the filename
            var normalizedName = fileName.Replace('\\', '_').Replace('/', '_');
            
            // Previous logic truncated the filename, which caused issues with "Email Demo/..." mismatch
            /*
            int lastSeparator = normalizedName.LastIndexOf('/');
            if (lastSeparator >= 0 && lastSeparator < normalizedName.Length - 1)
            {
                normalizedName = normalizedName.Substring(lastSeparator + 1);
            }
            */

            // First, determine the extension using the existing logic
            string extension = GetFileExtension(normalizedName);

            // Remove the extension from the filename
            string nameWithoutExtension = extension.Length > 0
                ? normalizedName.Substring(0, normalizedName.Length - extension.Length)
                : normalizedName;

            // Pattern to replace illegal characters (including slashes, though they should be gone now)
            string pattern = @"[~#%&*{}\\\/:,<>?\|\""']";
            var regex = new Regex(pattern);

            // Clean the filename part
            var cleaned = regex.Replace(nameWithoutExtension.Trim(), "_");

            // Collapse multiple underscores and trim from the ends for nicer names
            cleaned = Regex.Replace(cleaned, "_+", "_").Trim('_', ' ');

            // Recombine with extension
            return cleaned + extension;
        }
        private static string GetFileExtension(string fileName)
        {
            // Check if there's a valid extension
            int lastDotIndex = fileName.LastIndexOf('.');
            if (lastDotIndex == -1 || lastDotIndex == fileName.Length - 1)
            {
                // No dot or dot is the last character, no extension
                return string.Empty;
            }

            // Extract the supposed "extension"
            string possibleExtension = fileName.Substring(lastDotIndex);

            // Validate if it's a proper extension (e.g., no spaces or invalid characters)
            if (possibleExtension.Contains(" ") || possibleExtension.Contains("(") || possibleExtension.Contains(")"))
            {
                return string.Empty; // Not a valid extension
            }

            return possibleExtension;
        }
        private static (string filePath, bool isDuplicate) GetUniqueFilePath(string filePath)
        {
            if (!File.Exists(filePath))
                return (filePath, false);

            string directory = Path.GetDirectoryName(filePath);
            string fileName = Path.GetFileNameWithoutExtension(filePath);
            string extension = Path.GetExtension(filePath);
            int counter = 1;

            while (File.Exists(filePath))
            {
                filePath = Path.Combine(directory, $"{fileName}_C{counter}{extension}");
                counter++;
            }

            return (filePath, true);
        }
        public static string SanitizePathForLogs(string path, string fileNameWithExt = "")
        {
            if (string.IsNullOrWhiteSpace(path))
        return string.Empty;

    // Fix for paths where document names contain "text/\text" which might be split incorrectly.
    // Replace "/\" with "__" so it stays as one segment.
    if (path.Contains("/\\"))
    {
        path = path.Replace("/\\", "__");
    }

    // Use SplitPath to get segments (handles both '>' and '\' separators correctly)
    string[] pathParts = SplitPath(path);

            if (pathParts.Length == 0)
                return string.Empty;

            // Determine how many segments to include as folders
            int folderCount = pathParts.Length;
            
            // If fileNameWithExt is provided, check if the last segment is the document name
            if (!string.IsNullOrEmpty(fileNameWithExt))
            {
                string cleanedFileName = CleanFileName(fileNameWithExt);
                string fileNameBase = Path.GetFileNameWithoutExtension(cleanedFileName);
                string fileNameExt = Path.GetExtension(cleanedFileName);
                string lastSegment = pathParts[pathParts.Length - 1];
                
                // Extract the actual filename part from last segment (after last / or \)
                string lastSegmentFileName = lastSegment;
                int lastSlash = Math.Max(lastSegment.LastIndexOf('/'), lastSegment.LastIndexOf('\\'));
                if (lastSlash >= 0 && lastSlash < lastSegment.Length - 1)
                {
                    lastSegmentFileName = lastSegment.Substring(lastSlash + 1);
                }
                
                // Clean the extracted filename for comparison
                string lastSegmentCleaned = CleanFileName(lastSegmentFileName);
                string lastSegmentBase = Path.GetFileNameWithoutExtension(lastSegmentCleaned);
                
                // Check if last segment contains the document name (multiple ways to match)
                bool isDocumentName = 
                    lastSegment.Contains(cleanedFileName) ||  // Full cleaned name in segment
                    lastSegmentFileName.Contains(fileNameBase) ||  // Base name in filename part
                    lastSegmentCleaned.Equals(cleanedFileName, StringComparison.OrdinalIgnoreCase) ||  // Exact match after cleaning
                    (lastSegmentBase.Equals(fileNameBase, StringComparison.OrdinalIgnoreCase) && 
                     Path.GetExtension(lastSegmentCleaned).Equals(fileNameExt, StringComparison.OrdinalIgnoreCase));  // Base + extension match
                
                if (isDocumentName)
                {
                    folderCount = pathParts.Length - 1; // Exclude last segment (document name)
                }
            }
            else
            {
                // No fileName provided, assume last segment might be document name if it has an extension
                if (pathParts.Length > 1)
                {
                    string lastSegment = pathParts[pathParts.Length - 1];
                    // Check if last segment looks like a document name (has extension or contains / or \)
                    if (lastSegment.Contains('.') && (lastSegment.Contains('/') || lastSegment.Contains('\\')))
                    {
                        folderCount = pathParts.Length - 1; // Exclude last segment (likely document name)
                    }
                }
            }

            // Take only folder segments (exclude document name segment)
            var folderSegments = pathParts.Take(folderCount)
                .Select(s => CleanFolderSegment(s))
                .Where(s => !string.IsNullOrEmpty(s));
            
            // Join with '/' for SharePoint path format
            return string.Join("/", folderSegments);
        }

        private static string CleanFolderSegment(string segment)
        {
            // Replace separators with underscore to prevent creating extra folders
            string s = segment.Replace('/', '_').Replace('\\', '_');
            return CleanFilePath(s);
        }

        private static string GetDestinationPath(string sanitizePath, string fileName, string sharePointUrl)
        {
            string spUrl = sharePointUrl.TrimEnd('/');
            string cleanLogPath = string.IsNullOrEmpty(sanitizePath) ? "" : sanitizePath.Replace('\\', '/').Trim('/');
            
            // Check for duplication: if spUrl ends with the start of cleanLogPath
            string urlLastSegment = spUrl.Split('/').Last();
            if (!string.IsNullOrEmpty(urlLastSegment))
            {
                if (cleanLogPath.StartsWith(urlLastSegment + "/", StringComparison.OrdinalIgnoreCase))
                {
                    cleanLogPath = cleanLogPath.Substring(urlLastSegment.Length).TrimStart('/');
                }
                else if (cleanLogPath.Equals(urlLastSegment, StringComparison.OrdinalIgnoreCase))
                {
                    cleanLogPath = "";
                }
            }

            string destPath = "/" + spUrl.TrimStart('/') + (string.IsNullOrEmpty(cleanLogPath) ? "" : "/" + cleanLogPath) + "/" + fileName;
            return destPath.Replace("//", "/");
        }
        public static string[] SplitPath(string path)
        {
            if (string.IsNullOrWhiteSpace(path))
                return new string[0];

            // OTCS path from fn_llpathwsg uses '>' as separator: Enterprise>zTest>DLFilePlan>Folder 1>Email Demo/\_18112024_171540_1.msg>
            // If we split on '\', the backslash inside the document name would create a fake "Email Demo/" folder.
            // So when the path contains '>', split only on '>' so the document name (which may contain / or \) stays one segment.
            if (path.IndexOf('>') >= 0)
            {
                var segments = path.Split(new[] { '>' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(s => s.Trim())
                    .Where(s => !string.IsNullOrEmpty(s))
                    .ToArray();
                return segments;
            }

            // Original logic: path uses '\' as separator (e.g. from other sources)
            var result = new List<string>();
            int startIndex = 0;
            bool inBrackets = false;

            for (int i = 0; i < path.Length; i++)
            {
                if (path[i] == '<')
                {
                    inBrackets = true;
                    continue;
                }
                if (path[i] == '\\')
                {
                    if (inBrackets)
                    {
                        inBrackets = false;
                        continue;
                    }

                    bool isProbablySeparator = true;

                    if (i > 0 && i < path.Length - 1)
                    {
                        bool isPartOfOperator = false;

                        if (i < path.Length - 1 && path[i + 1] == '=')
                            isPartOfOperator = true;

                        if (i > 0 && path[i - 1] == '<')
                            isPartOfOperator = true;

                        bool hasPreviousContent = i > 0 && !char.IsWhiteSpace(path[i - 1]);
                        bool hasNextContent = i < path.Length - 1 && !char.IsWhiteSpace(path[i + 1]);

                        isProbablySeparator = hasPreviousContent && hasNextContent && !isPartOfOperator;
                    }

                    if (isProbablySeparator)
                    {
                        if (i > startIndex)
                        {
                            var segment = path.Substring(startIndex, i - startIndex).Trim();
                            result.Add(segment);
                        }
                        startIndex = i + 1;
                    }
                }
            }

            if (startIndex < path.Length)
            {
                string finalSegment = path.Substring(startIndex).Trim();
                result.Add(finalSegment);
            }

            return result.ToArray();
        }

        private static string GetUniqueFilePathBeforWrite(string filePath)
        {
            string directory = Path.GetDirectoryName(filePath);
            string fileName = Path.GetFileNameWithoutExtension(filePath);
            string extension = Path.GetExtension(filePath);
            int counter = 1;

            while (File.Exists(filePath))
            {
                filePath = Path.Combine(directory, $"{fileName}_C{counter}{extension}");
                counter++;
            }

            return filePath;
        }

        private static string GetLongFilePath(string filePath, long nodeID)
        {
            string directory = Properties.Settings.Default.LongFilePathLocation;
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }
            string fileName = Path.GetFileNameWithoutExtension(filePath);
            string extension = Path.GetExtension(filePath);

            filePath = Path.Combine(directory, $"{nodeID}{extension}");

            return filePath;
        }

        private static void LogErrorToFile(string failLogPath, CustomNode node, Exception ex, DateTime startTime)
        {
            var errorFileName = $"ErrorLog_{startTime:yyyyMMdd-HHmmss}.txt";
            var errorFilePath = Path.Combine(failLogPath, errorFileName);

            var errorMessage = $"Error downloading {node.Path}\\{node.Node.Name}\n" +
                              $"NodeID: {node.Node.ID}\n" +
                              $"Error: {ex.Message}\n" +
                              $"StackTrace: {ex.StackTrace}\n" +
                              $"Timestamp: {DateTime.Now}\n" +
                              $"-----------------------------------\n";

            File.AppendAllText(errorFilePath, errorMessage);
        }
        private static async Task ProcessDownloadsAsync(List<CustomNode> downloadFolders, string baseFilePath, string token, DownloadDatabase db, DateTime tokenTime, string userName, string password, string faillog, bool isResume, DateTime startTime)
        {
            int totalItemsProcessed = 0;
            int successfulDownloads = 0;
            int failedDownloads = 0;
            var newFormats = new List<NewFormat>();

            var httpClient = new HttpClient();
            const int timeoutSeconds = 900; // 5 minutes timeout
            httpClient.Timeout = TimeSpan.FromSeconds(timeoutSeconds);

            var downloadQueue = new BlockingCollection<DownloadTask>();

            // Read the max thread count from config
            int maxThreads = Properties.Settings.Default.MaxThreads;
            if (maxThreads == 0)
            {
                maxThreads = Environment.ProcessorCount; // Default to number of processors
            }
            // Use SemaphoreSlim with configurable thread count
            var semaphore = new SemaphoreSlim(maxThreads);

            long lastDownloadedId = isResume ? db.GetLastDownloadedRecordId() : 0;

            // Populate the download queue
            foreach (var df in downloadFolders)
            {
                if ((df.Node.DisplayType == "144" || df.Node.DisplayType == "749") &&
                    (!isResume || df.Node.ID > lastDownloadedId))
                {
                    downloadQueue.Add(new DownloadTask { Node = df });
                }
            }

            // Mark the queue as complete for adding
            downloadQueue.CompleteAdding();

            // Process the download queue
            var tasks = new List<Task>();
            foreach (var task in downloadQueue.GetConsumingEnumerable())
            {
                await semaphore.WaitAsync();
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        totalItemsProcessed++;
                        string filePath = GetNextAvailableFilePath(baseFilePath, task.Node.Node.Name, task.Node.Node.ID);
                        var downloadedStatus = await DownloadNodeAsync(task.Node, token, filePath, httpClient);
                        if (downloadedStatus.Success)
                        {
                            successfulDownloads++;

                            //if (IsBlockedExtension(filePath))
                            //{
                            //    filePath = await ZipFileAsync(filePath);
                            //}

                            string sourceUrl = Properties.Settings.Default.SourceUrl; // http://192.168.1.113/otcs/cs.exe/app/nodes/{0}
                            var newFormat = new NewFormat()
                            {
                                Source = String.Format(sourceUrl, task.Node.Node.ID),
                                SourceDocLib = string.Empty,
                                SourceSubFolder = task.Node.Path
                            };
                            newFormats.Add(newFormat);
                        }
                        else
                        {
                            failedDownloads++;
                        }

                        var record = new DownloadRecord
                        {
                            Id = task.Node.Node.ID,
                            ParentId = task.Node.Node.ParentID,
                            Name = task.Node.Node.Name,
                            DisplayType = task.Node.Node.DisplayType,
                            Version = task.Node.Version,
                            Downloaded = downloadedStatus.Success
                        };

                        db.InsertOrUpdateRecord(record);
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }));
            }

            await Task.WhenAll(tasks);

            // Write to CSV
            string baseReportPath = Properties.Settings.Default.CsvOutput;
            string csvPath = Path.Combine(baseReportPath, StartTime.ToString("yyyyMMdd-HHmmss") + "_SourceFor " + parentNode.Name + ".csv");
            using (var writer = new StreamWriter(csvPath))
            using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
            {
                csv.WriteRecords(newFormats);
            }

            Logger.Info("Download process completed.");
            Logger.Info($"Total items processed: {totalItemsProcessed}");
            Logger.Info($"Successful downloads: {successfulDownloads}");
            Logger.Info($"Failed downloads: {failedDownloads}");
        }

        private static string GetNextAvailableFilePath(string baseFilePath, string fileName, long nodeId)
        {
            int folderNumber = 1;
            string subfolderName;
            string folderPath;

            // Sanitize the file name
            fileName = SanitizeFileName(fileName);

            while (true)
            {
                subfolderName = $"{Path.GetFileName(baseFilePath)}_{folderNumber}";
                folderPath = Path.Combine(baseFilePath, subfolderName);

                int currentCount = folderItemCounts.AddOrUpdate(folderPath, 1, (key, oldValue) => oldValue + 1);

                int maxItemsPerFolder = Settings.Default.MaxItemsPerFolder;
                if (maxItemsPerFolder == 0)
                {
                    maxItemsPerFolder = 1000; // Default
                }
                if (currentCount <= maxItemsPerFolder)
                {
                    Directory.CreateDirectory(folderPath);
                    string filePath = Path.Combine(folderPath, fileName);

                    // Check for duplicate filename
                    if (File.Exists(filePath))
                    {
                        string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
                        string extension = Path.GetExtension(fileName);
                        filePath = Path.Combine(folderPath, $"{fileNameWithoutExtension}_{nodeId}{extension}");
                    }

                    return filePath;
                }

                // Move to next subfolder
                folderNumber++;
            }
        }

        private static string SanitizeFileName(string fileName)
        {
            // Define the characters to be replaced
            string[] invalidChars = { "~", "#", "%", "&", "*", "{", "}", "\\", ":", "<", ">", "?", "/", "|", "\"" };

            // Replace each invalid character with an underscore
            foreach (string c in invalidChars)
            {
                fileName = fileName.Replace(c, "_");
            }

            // Replace consecutive underscores with a single underscore
            fileName = System.Text.RegularExpressions.Regex.Replace(fileName, @"_{2,}", "_");

            // Trim leading and trailing underscores
            fileName = fileName.Trim('_');

            // Ensure the filename is not empty after sanitization
            if (string.IsNullOrWhiteSpace(fileName))
            {
                fileName = "File_" + Guid.NewGuid().ToString();
            }

            return fileName;
        }

        private static (bool IsBlocked, string Reason) IsBlockedExtension(string filePath)
        {
            bool isBlocked = false;
            string reason = string.Empty;

            string[] blockedExtensions = Properties.Settings.Default.BlockedExtensions.Split(',');
            string extension = Path.GetExtension(filePath).ToLower();
            string fileName = Path.GetFileName(filePath);
            string blockedStartWithString = Properties.Settings.Default.BlockedStartWith;

            if (blockedExtensions.Contains(extension))
            {
                isBlocked = true;
                reason += $"File '{fileName}' ends with blocked extension '{extension}'. ";
            }
            if (fileName.StartsWith(blockedStartWithString))
            {
                isBlocked = true;
                reason += $"File '{fileName}' starts with blocked string '{blockedStartWithString}'. ";
            }

            return (isBlocked, reason.Trim());
        }

        private static (bool IsBlocked, string Reason) IsBlockedFolder(string folderPath)
        {
            bool isBlocked = false;
            string reason = string.Empty;

            string[] blockedExtensions = Properties.Settings.Default.BlockedExtensions.Split(',');
            string blockedStartWithString = Properties.Settings.Default.BlockedStartWith;
            string folderName = Path.GetFileName(folderPath);

            // Check if folder ends with any blocked suffix
            string matchingSuffix = blockedExtensions.FirstOrDefault(suffix =>
                folderName.EndsWith(suffix, StringComparison.OrdinalIgnoreCase));
            if (matchingSuffix != null)
            {
                isBlocked = true;
                reason += $"Folder '{folderName}' ends with blocked suffix '{matchingSuffix}'. ";
            }

            // Check if folder starts with the blocked string
            if (folderName.StartsWith(blockedStartWithString))
            {
                isBlocked = true;
                reason += $"Folder '{folderName}' starts with blocked string '{blockedStartWithString}'. ";
            }

            return (isBlocked, reason.Trim());
        }

        private static async Task<string> ZipFileAsync(string filePath)
        {
            string zipPath = Path.ChangeExtension(filePath, ".zip");
            string password = Properties.Settings.Default.ZipPassword;

            using (FileStream fsOut = File.Create(zipPath))
            using (ZipOutputStream zipStream = new ZipOutputStream(fsOut))
            {
                zipStream.SetLevel(3); //0-9, 9 being the highest level of compression
                zipStream.Password = password;

                byte[] buffer = new byte[4096];
                ZipEntry entry = new ZipEntry(Path.GetFileName(filePath));
                entry.DateTime = DateTime.Now;
                zipStream.PutNextEntry(entry);

                using (FileStream fsIn = File.OpenRead(filePath))
                {
                    int sourceBytes;
                    do
                    {
                        sourceBytes = await fsIn.ReadAsync(buffer, 0, buffer.Length);
                        await zipStream.WriteAsync(buffer, 0, sourceBytes);
                    } while (sourceBytes > 0);
                }

                zipStream.CloseEntry();
            }

            File.Delete(filePath);
            return zipPath;
        }

        private static void ConfigureNLog()
        {
            var directory = Path.Combine(Path.GetDirectoryName(Process.GetCurrentProcess().MainModule.FileName), "Logs");

            // Configure NLog
            var config = new NLog.Config.LoggingConfiguration();

            // Create colored console target
            var logconsole = new NLog.Targets.ColoredConsoleTarget("logconsole")
            {
                Layout = "${longdate}|${message}" // Only show the timestamp and the message
            };

            // Set different colors based on log level
            logconsole.RowHighlightingRules.Add(new NLog.Targets.ConsoleRowHighlightingRule
            {
                Condition = "level == LogLevel.Fatal",
                ForegroundColor = NLog.Targets.ConsoleOutputColor.Red,
                BackgroundColor = NLog.Targets.ConsoleOutputColor.White
            });

            logconsole.RowHighlightingRules.Add(new NLog.Targets.ConsoleRowHighlightingRule
            {
                Condition = "level == LogLevel.Error",
                ForegroundColor = NLog.Targets.ConsoleOutputColor.Red
            });

            logconsole.RowHighlightingRules.Add(new NLog.Targets.ConsoleRowHighlightingRule
            {
                Condition = "level == LogLevel.Warn",
                ForegroundColor = NLog.Targets.ConsoleOutputColor.Yellow
            });

            logconsole.RowHighlightingRules.Add(new NLog.Targets.ConsoleRowHighlightingRule
            {
                Condition = "level == LogLevel.Info",
                ForegroundColor = NLog.Targets.ConsoleOutputColor.Green
            });

            logconsole.RowHighlightingRules.Add(new NLog.Targets.ConsoleRowHighlightingRule
            {
                Condition = "level == LogLevel.Debug",
                ForegroundColor = NLog.Targets.ConsoleOutputColor.Gray
            });

            logconsole.RowHighlightingRules.Add(new NLog.Targets.ConsoleRowHighlightingRule
            {
                Condition = "level == LogLevel.Trace",
                ForegroundColor = NLog.Targets.ConsoleOutputColor.DarkGray
            });

            config.AddRule(LogLevel.Info, LogLevel.Fatal, logconsole);

            // Create file target with StartTime in the filename
            var logfile = new NLog.Targets.FileTarget("logfile")
            {
                FileName = Path.Combine(directory, $"{StartTime:yyyyMMdd-HHmmss}_Log.txt"),
                Layout = "${longdate}|${level:uppercase=true}|${logger}|${message}"
            };
            config.AddRule(LogLevel.Trace, LogLevel.Fatal, logfile);

            // Apply config
            LogManager.Configuration = config;
        }
        public static bool DoesKeyExists(string containerName)
        {
            var cspParams = new CspParameters
            {
                Flags = CspProviderFlags.UseExistingKey,
                KeyContainerName = containerName
            };

            try
            {
                var provider = new RSACryptoServiceProvider(cspParams);
            }
            catch (Exception e)
            {
                return false;
            }
            return true;
        }
        private static void parseDownloadNested(long root, ref List<CustomNode> newFolders, int level, ref string token, ref DateTime tokenTime, string userName, string password, string filePath, string faillog, string downloadNestedJSON)
        {
            var thisNode = newFolders.Find(delegate (CustomNode nd) { return nd.Node.ID == root; });


            var theseNodes = newFolders.FindAll(delegate (CustomNode nd)
            {
                return nd.Node.ParentID == root;
            });
            // Always create directory for TargetedNode and child nodes, or if ExtractTargetedNode is true
            if (thisNode.Node.ID != Settings.Default.TargetedNode || Settings.Default.ExtractTargetedNode)
            {
                filePath = filePath + Path.DirectorySeparatorChar + thisNode.Node.Name;
                Directory.CreateDirectory(filePath);
                level++;
            }
            foreach (var nodeWithCat in theseNodes)
            {
                if (nodeWithCat.Node.DisplayType == "0" || nodeWithCat.Node.DisplayType == "751")
                {
                    var folderNode = nodeWithCat;
                    Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Found folder " + folderNode.Node.Name);
                    Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Checking for child folders in " +
                                    folderNode.Node.Name);
                    parseDownloadNested(folderNode.Node.ID, ref newFolders, level, ref token, ref tokenTime, userName, password, filePath, faillog, downloadNestedJSON);
                }
                else
                {
                    var fileNode = nodeWithCat;
                    if (!fileNode.Downloaded)
                    {
                        Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Found file " + fileNode.Node.Name);
                        var newFilePath = filePath + Path.DirectorySeparatorChar + fileNode.Node.Name;
                        var downloaded = DownloadNode(fileNode, token, ref tokenTime, newFilePath, userName, password,
                            faillog, true);
                        nodeWithCat.Downloaded = downloaded;
                    }
                    else
                    {
                        Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Skipping file " + fileNode.Node.Name);
                    }
                    var jsonDF = new List<JsonNode>();
                    foreach (var folder in newFolders)
                    {
                        var jsonNode = new JsonNode();
                        jsonNode.ParentID = folder.Node.ParentID;
                        jsonNode.ID = folder.Node.ID;
                        jsonNode.name = folder.Node.Name;
                        jsonNode.DisplayType = folder.Node.DisplayType;
                        jsonNode.Version = folder.Version;
                        jsonNode.Downloaded = folder.Downloaded;
                        jsonDF.Add(jsonNode);
                    }
                    string json = JsonConvert.SerializeObject(jsonDF, Formatting.None);
                    using (StreamWriter outputFile = new StreamWriter(downloadNestedJSON))
                    {
                        outputFile.WriteLine(json);
                    }
                }
            }
        }
        private static void parseDownload(long root, ref List<CustomNode> newFolders, int level, ref int foldercount, ref string token, ref DateTime tokenTime, string userName, string password, string filePath, string faillog, string doDownloadJSON)
        {
            var thisNode = newFolders.Find(delegate (CustomNode nd) { return nd.Node.ID == root; });


            var theseNodes = newFolders.FindAll(delegate (CustomNode nd)
            {
                return nd.Node.ParentID == root;
            });
            if (thisNode.Node.ID != Settings.Default.TargetedNode)
            {
                //baseFilePath = baseFilePath + Path.DirectorySeparator + "lvl " + level;
                //Directory.CreateDirectory(baseFilePath);
                //level++;
            }
            foreach (var nodeWithCat in theseNodes)
            {
                if (nodeWithCat.Node.DisplayType == "0" || nodeWithCat.Node.DisplayType == "751")
                {
                    var folderNode = nodeWithCat;
                    Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Found folder " + folderNode.Node.Name);
                    Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Checking for child folders in " +
                                    folderNode.Node.Name);
                    parseDownload(folderNode.Node.ID, ref newFolders, level, ref foldercount, ref token, ref tokenTime, userName, password, filePath, faillog, doDownloadJSON);
                }
                else
                {
                    var fileNode = nodeWithCat;
                    if (!fileNode.Downloaded)
                    {
                        Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Found file " + fileNode.Node.Name);
                        var dirFilePath = filePath + Path.DirectorySeparatorChar + fileNode.Node.ID;
                        Directory.CreateDirectory(dirFilePath);
                        var newFilePath = dirFilePath + Path.DirectorySeparatorChar + fileNode.Node.Name;
                        var downloaded = DownloadNode(fileNode, token, ref tokenTime, newFilePath, userName, password, faillog, false);
                        nodeWithCat.Downloaded = downloaded;
                    }
                    else
                    {
                        Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Skipping file " + fileNode.Node.Name);
                    }
                    var jsonDF = new List<JsonNode>();
                    foreach (var folder in newFolders)
                    {
                        var jsonNode = new JsonNode();
                        jsonNode.ParentID = folder.Node.ParentID;
                        jsonNode.ID = folder.Node.ID;
                        jsonNode.name = folder.Node.Name;
                        jsonNode.DisplayType = folder.Node.DisplayType;
                        jsonNode.Version = folder.Version;
                        jsonNode.Downloaded = folder.Downloaded;
                        jsonDF.Add(jsonNode);
                    }
                    string json = JsonConvert.SerializeObject(jsonDF, Formatting.None);
                    using (StreamWriter outputFile = new StreamWriter(doDownloadJSON))
                    {
                        outputFile.WriteLine(json);
                    }
                    foldercount++;
                }
            }
        }

        private static void parseDownloadParallel(CustomNode thisNode, ref List<CustomNode> newFolders, ref string token, ref DateTime tokenTime, string userName, string password, string filePath, string faillog, string doDownloadJSON)
        {
            if (thisNode.Node.DisplayType != "0" && thisNode.Node.DisplayType != "751" && thisNode.Node.DisplayType != "141")
            {
                var fileNode = thisNode;
                if (!fileNode.Downloaded)
                {
                    Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Found file " + fileNode.Node.Name);
                    var dirFilePath = filePath + Path.DirectorySeparatorChar + fileNode.Node.ID;
                    Directory.CreateDirectory(dirFilePath);
                    var newFilePath = dirFilePath + Path.DirectorySeparatorChar + fileNode.Node.Name;
                    var downloaded = DownloadNode(fileNode, token, ref tokenTime, newFilePath, userName, password, faillog, false);
                    thisNode.Downloaded = downloaded;

                    // Update JSON file after each successfull download
                    UpdateJsonFile(thisNode, doDownloadJSON);
                }
                else
                {
                    Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Skipping file " + fileNode.Node.Name);
                }
            }
        }
        private static void ProcessDownloadQueue(string token, DownloadDatabase db, ref DateTime tokenTime, string userName, string password, string faillog, string doDownloadJSON)
        {
            foreach (var task in downloadQueue.GetConsumingEnumerable())
            {
                bool downloaded = DownloadNode(task.Node, token, ref tokenTime, task.FilePath, userName, password, faillog, false);
                task.Node.Downloaded = downloaded;
                // Insert record to download database
                db.InsertOrUpdateRecord(new DownloadRecord
                {
                    Id = task.Node.Node.ID,
                    ParentId = task.Node.Node.ParentID,
                    Name = task.Node.Node.Name,
                    DisplayType = task.Node.Node.DisplayType,
                    Version = task.Node.Version,
                    Downloaded = downloaded
                });
            }
        }
        private static void UpdateJsonFile(CustomNode node, string doDownloadJSON)
        {
            var jsonNode = new JsonNode
            {
                ParentID = node.Node.ParentID,
                ID = node.Node.ID,
                name = node.Node.Name,
                DisplayType = node.Node.DisplayType,
                Version = node.Version,
                Downloaded = node.Downloaded
            };

            lock (fileLock)
            {
                List<JsonNode> existingNodes = JSONManager.LoadDownloadRecords(doDownloadJSON);

                // Update or add the current Node
                var existingNode = existingNodes.FirstOrDefault(n => n.ID == jsonNode.ID);
                if (existingNode != null)
                {
                    existingNode.Downloaded = jsonNode.Downloaded;
                    existingNode.Version = jsonNode.Version;
                }
                else
                {
                    existingNodes.Add(jsonNode);
                }
                JSONManager.SaveDownloadRecords(existingNodes, doDownloadJSON);
            }
        }
        private static bool DownloadNode(CustomNode thisNode, string token, ref DateTime tokenTime, string filePath, string username, string password, string faillog, bool isNested)
        {
            var downloaded = true;

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();
            Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Download for " + thisNode.Node.Name + " (" + thisNode.Node.ID.ToString() + ") starting.");

            try
            {
                RestClient client = new RestClient(Properties.Settings.Default.APIUrl + "v1/nodes/" + thisNode.Node.ID.ToString() + "/versions/" + thisNode.Version.ToString() + "/content");
                //RestClient client = new RestClient(Properties.Settings.Default.APIUrl + "v1/nodes" + thisNode.Node.ID.ToString() + "/content");
                RestRequest request = new RestRequest("", Method.Get);
                request.AddHeader("OTDSTicket", AuthAPI.AuthenticateUserViaOTDS());

                byte[] response = client.DownloadData(request);
                File.WriteAllBytes(filePath, response);

                long elapsedMillseconds = stopWatch.ElapsedMilliseconds;
                string unit = "ms";

                if (elapsedMillseconds > 60000)
                {
                    elapsedMillseconds /= 60000; // in minutes
                    unit = "minutes";
                }
                else if (elapsedMillseconds > 1000)
                {
                    elapsedMillseconds /= 1000; // in seconds
                    unit = "seconds";
                }

                Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|" + thisNode.Node.Name + " (" + thisNode.Node.ID + ") download successful. Time taken: " + elapsedMillseconds + " " + unit);
            }
            catch (Exception e)
            {
                var error1 = DateTime.Now.ToString("HH:mm:ss") + "|Can't download Node " + e.Message;
                var error2 = DateTime.Now.ToString("HH:mm:ss") + "|Can't download Node " + thisNode.Node.ID +
                             " with the name of " + thisNode.Node.Name + " and a display type of " +
                             thisNode.TranslatedDisplayType + ". It is probably not a file type";
                Trace.WriteLine(error1);
                Trace.WriteLine(error2);

                lock (logLock)
                {
                    using (var sw = new StreamWriter(Path.Combine(faillog, errorlogfilename), true))
                    {
                        sw.WriteLine(error1);
                        sw.WriteLine(error2);
                    }
                }

                downloaded = false;
            }

            stopWatch.Stop();
            return downloaded;
        }
        public static async Task<(bool Success, string ErrorMessage)> DownloadNodeAsync(CustomNode thisNode, string token, string filePath, HttpClient httpClient)
        {
            const int bufferSize = 81920; // 80KB buffer                        

            var stopWatch = new Stopwatch();
            stopWatch.Start();
            Logger.Info($"Download {thisNode.Node.Name} ({thisNode.Node.ID}) starting...");

            try
            {
                token = await TokenManager.GetValidTokenAsync();
                string url = $"{Properties.Settings.Default.APIUrl}v1/nodes/{thisNode.Node.ID}/versions/{thisNode.Version}/content";
                using (var request = new HttpRequestMessage(HttpMethod.Get, url))
                {
                    request.Headers.Add("OTDSTicket", token);

                    using (var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead))
                    {
                        if (!response.IsSuccessStatusCode)
                        {
                            string errorContent = await response.Content.ReadAsStringAsync();
                            string errorMessage = $"HTTP Error {(int)response.StatusCode} ({response.StatusCode}): {errorContent}";
                            Logger.Info($"Download failed for Node {thisNode.Node.ID}: {errorMessage}");
                            return (false, errorMessage);
                        }
                        long? contentLength = response.Content.Headers.ContentLength;


                        if (File.Exists(filePath))
                        {
                            filePath = GetUniqueFilePathBeforWrite(filePath);
                        }

                        using (var contentStream = await response.Content.ReadAsStreamAsync())
                        {
                            using (var fileStream = new FileStream(filePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, bufferSize, useAsync: true))
                            {
                                byte[] buffer = new byte[bufferSize];
                                long totalBytesRead = 0;
                                int bytesRead;
                                while ((bytesRead = await contentStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                                {
                                    await fileStream.WriteAsync(buffer, 0, bytesRead);
                                    totalBytesRead += bytesRead;

                                    // Optional: Report progress
                                    if (contentLength.HasValue)
                                    {
                                        double progressPercentage = (double)totalBytesRead / contentLength.Value * 100;
                                        Logger.Info($"Download progress for {thisNode.Node.Name}: {progressPercentage:F2}%");
                                    }
                                }
                            }
                        }

                        TimeSpan elapsed = stopWatch.Elapsed;
                        string unit = "ms";
                        double elapsedTime = elapsed.TotalMilliseconds;

                        if (elapsed.TotalMinutes >= 1)
                        {
                            elapsedTime = elapsed.TotalMinutes;
                            unit = "minutes";
                        }
                        else if (elapsed.TotalSeconds >= 1)
                        {
                            elapsedTime = elapsed.TotalSeconds;
                            unit = "seconds";
                        }

                        Logger.Info($"Item: {thisNode.Node.Name} ({thisNode.Node.ID}) download successful. Time taken: {elapsedTime:F2} {unit}");
                        return (true, string.Empty);
                    }
                }
            }
            catch (Exception e)
            {
                string errorMessage = $"Can't download Node {thisNode.Node.ID} with the name of {thisNode.Node.Name} and a display type of {thisNode.TranslatedDisplayType}. Error message: {e.Message}";
                Logger.Info(errorMessage);
                Logger.Trace($"Stack trace message: {e.StackTrace}");
                return (false, errorMessage);
            }
            finally
            {
                stopWatch.Stop();
            }
        }
        private static void EncryptFile(string filePath)
        {
            var file = new FileInfo(filePath);
            var fileE = new FileInfo(Path.Combine(file.DirectoryName, Path.GetFileNameWithoutExtension(file.Name)));
            var transform = aes.CreateEncryptor();

            var keyEncrypted = rsa.Encrypt(aes.Key, false);

            byte[] LenK = new byte[4];
            byte[] LenIV = new byte[4];

            int lKey = keyEncrypted.Length;
            LenK = BitConverter.GetBytes(lKey);
            int lIV = aes.IV.Length;
            LenIV = BitConverter.GetBytes(lIV);

            using (FileStream outFs = new FileStream(fileE.FullName, FileMode.Create))
            {
                outFs.Write(LenK, 0, 4);
                outFs.Write(LenIV, 0, 4);
                outFs.Write(keyEncrypted, 0, lKey);
                outFs.Write(aes.IV, 0, lIV); using (CryptoStream outStreamEncrypted = new CryptoStream(outFs, transform, CryptoStreamMode.Write))
                {
                    int count = 0;
                    int offset = 0;

                    int blockSizeBytes = aes.BlockSize / 8;
                    byte[] data = new byte[blockSizeBytes];
                    int bytesRead = 0;

                    using (FileStream inFs = new FileStream(file.FullName, FileMode.Open))
                    {
                        do
                        {
                            count = inFs.Read(data, 0, blockSizeBytes);
                            offset += count;
                            outStreamEncrypted.Write(data, 0, count);
                            bytesRead += blockSizeBytes;
                        }
                        while (count > 0);
                        inFs.Close();
                    }
                    outStreamEncrypted.FlushFinalBlock();
                    outStreamEncrypted.Close();
                }
                outFs.Close();
            }
            File.Delete(file.FullName);
        }
        private static void EncryptFileAndFileName(string filePath)
        {
            var file = new FileInfo(filePath);
            var fileE = new FileInfo(Path.Combine(file.DirectoryName, sha256_hash(file.Name)));
            var transform = aes.CreateEncryptor();

            var keyEncrypted = rsa.Encrypt(aes.Key, false);

            byte[] LenK = new byte[4];
            byte[] LenIV = new byte[4];

            int lKey = keyEncrypted.Length;
            LenK = BitConverter.GetBytes(lKey);
            int lIV = aes.IV.Length;
            LenIV = BitConverter.GetBytes(lIV);

            using (FileStream outFs = new FileStream(fileE.FullName, FileMode.Create))
            {
                outFs.Write(LenK, 0, 4);
                outFs.Write(LenIV, 0, 4);
                outFs.Write(keyEncrypted, 0, lKey);
                outFs.Write(aes.IV, 0, lIV); using (CryptoStream outStreamEncrypted = new CryptoStream(outFs, transform, CryptoStreamMode.Write))
                {
                    int count = 0;
                    int offset = 0;

                    int blockSizeBytes = aes.BlockSize / 8;
                    byte[] data = new byte[blockSizeBytes];
                    int bytesRead = 0;

                    using (FileStream inFs = new FileStream(file.FullName, FileMode.Open))
                    {
                        do
                        {
                            count = inFs.Read(data, 0, blockSizeBytes);
                            offset += count;
                            outStreamEncrypted.Write(data, 0, count);
                            bytesRead += blockSizeBytes;
                        }
                        while (count > 0);
                        inFs.Close();
                    }
                    outStreamEncrypted.FlushFinalBlock();
                    outStreamEncrypted.Close();
                }
                outFs.Close();
            }
            File.Delete(file.FullName);
        }
        private static void DecryptFile(string filePath)
        {
            /*
            var fileE = new FileInfo(baseFilePath);
            var fileD = new FileInfo(Path.Combine(dir, Path.GetFileNameWithoutExtension(fileE.Name) + "D.jpg"));
            // Construct the file name for the decrypted file.
            var aes = Aes.Create();
            aes.GenerateIV();
            aes.GenerateKey();
            var LenK = new byte[4];
            var LenIV = new byte[4];
            // Use FileStream objects to read the encrypted
            // file (inFs) and save the decrypted file (outFs).
            using (FileStream inFs = new FileStream(fileE.FullName, FileMode.Open))
            {

                inFs.Seek(0, SeekOrigin.Begin);
                inFs.Seek(0, SeekOrigin.Begin);
                inFs.Read(LenK, 0, 3);
                inFs.Seek(4, SeekOrigin.Begin);
                inFs.Read(LenIV, 0, 3);

                // Convert the lengths to integer values.
                int lenK = BitConverter.ToInt32(LenK, 0);
                int lenIV = BitConverter.ToInt32(LenIV, 0);

                // Determine the start postition of
                // the ciphter text (startC)
                // and its length(lenC).
                int startC = lenK + lenIV + 8;
                int lenC = (int)inFs.Length - startC;

                // Create the byte arrays for
                // the encrypted Aes key,
                // the IV, and the cipher text.
                byte[] KeyEncrypted = new byte[lenK];
                byte[] IV = new byte[lenIV];

                // Extract the key and IV
                // starting from index 8
                // after the length values.
                inFs.Seek(8, SeekOrigin.Begin);
                inFs.Read(KeyEncrypted, 0, lenK);
                inFs.Seek(8 + lenK, SeekOrigin.Begin);
                inFs.Read(IV, 0, lenIV);
                // Use RSACryptoServiceProvider
                // to decrypt the AES key.
                byte[] KeyDecrypted = rsa.Decrypt(KeyEncrypted, false);

                // Decrypt the key.
                var transform = aes.CreateDecryptor(KeyDecrypted, IV);

                // Decrypt the cipher text from
                // from the FileSteam of the encrypted
                // file (inFs) into the FileStream
                // for the decrypted file (outFs).
                using (FileStream outFs = new FileStream(fileD.FullName, FileMode.Create))
                {

                    int count = 0;
                    int offset = 0;

                    // blockSizeBytes can be any arbitrary size.
                    int blockSizeBytes = aes.BlockSize / 8;
                    byte[] data = new byte[blockSizeBytes];

                    // By decrypting a chunk a time,
                    // you can save memory and
                    // accommodate large files.

                    // Start at the beginning
                    // of the cipher text.
                    inFs.Seek(startC, SeekOrigin.Begin);
                    using (CryptoStream outStreamDecrypted = new CryptoStream(outFs, transform, CryptoStreamMode.Write))
                    {
                        do
                        {
                            count = inFs.Read(data, 0, blockSizeBytes);
                            offset += count;
                            outStreamDecrypted.Write(data, 0, count);
                        }
                        while (count > 0);

                        outStreamDecrypted.FlushFinalBlock();
                        outStreamDecrypted.Close();
                    }
                    outFs.Close();
                }
                inFs.Close();
            }
            sw.Stop();
            Console.WriteLine((double)sw.ElapsedMilliseconds / 1000 + " Seconds");
            */
        }
        public static String sha256_hash(String value)
        {
            using (SHA256 hash = SHA256Managed.Create())
            {
                return String.Concat(hash
                    .ComputeHash(Encoding.UTF8.GetBytes(value))
                    .Select(item => item.ToString("x2")));
            }
        }

        private static void parse(long root, ref List<CustomNode> newFolders, int level, ref int foldercount,
            int highestlevel, List<string> permList, ref string token,
            SubTypesTranslator Translator, ref DateTime tokenTime, string userName, string password)
        {
            var thisNode = newFolders.Find(delegate (CustomNode nd) { return nd.Node.ID == root; });
            var theseNodes = newFolders.FindAll(delegate (CustomNode nd)
            {
                return nd.Node.ParentID == root;
            });
            if (thisNode != null)
                Translator.ConvertNodeDisplayTypeToString(ref thisNode, ref token, ref tokenTime, userName, password);
            {
                ParseFolder(level, ref foldercount, highestlevel, thisNode, permList, token);
                level++;
            }
            foreach (var nodeWithCat in theseNodes)
            {
                if (nodeWithCat.Node.DisplayType == "0" || nodeWithCat.Node.DisplayType == "751")
                {
                    var folderNode = nodeWithCat;
                    parse(folderNode.Node.ID, ref newFolders, level, ref foldercount, highestlevel, permList, ref token, Translator, ref tokenTime, userName, password);
                }
                else
                {
                    var fileNode = nodeWithCat;

                    Translator.ConvertNodeDisplayTypeToString(ref fileNode, ref token, ref tokenTime, userName, password);
                    ParseFolder(level, ref foldercount, highestlevel, fileNode, permList, token);
                }
            }
        }

        private static void ParseFolder(int key, ref int foldercount,
            int highestlevel,
            CustomNode folder, List<string> permList, string token)
        {
            try
            {
                var fileToWriteTo = folder.Path.Length > 400 ? longPathFileDT : filePlanDT;
                //add Permissions
                permList = ExtractPermission(folder);
                //add categories
                var catValuesKVP = folder.CatValues;

                longPathFileDT.Rows.Add();
                filePlanDT.Rows.Add();

                var itemArray = new string[18 + highestlevel];

                itemArray[highestlevel + 1] = folder.TranslatedDisplayType;

                string pathText = folder.Path.Length > 11 ? folder.Path.Substring(11) : folder.Path;

                itemArray[highestlevel + 2] = pathText;
                itemArray[highestlevel + 3] = folder.Node.Comment;

                for (var arrayIndex = 0; arrayIndex < permList.Count; arrayIndex++)
                {
                    itemArray[arrayIndex + highestlevel + 4] = permList[arrayIndex];
                }

                itemArray[0] = folder.Node.ID.ToString();
                itemArray[key + 1] = folder.Node.Name;
                fileToWriteTo.Rows[foldercount].ItemArray = itemArray;

                var newRow = false;
                var catHeadIndex = highestlevel + 16;
                var catValIndex = highestlevel + 17;
                foreach (KeyValuePair<int, string> kvp in catValuesKVP)
                {
                    if (kvp.Key == 1)
                    {
                        if (catValuesKVP.IndexOf(kvp) != 0)
                        {
                            itemArray[catValIndex] += "\"";
                            fileToWriteTo.Rows[foldercount].ItemArray = itemArray;
                        }
                        if (newRow)
                        {
                            fileToWriteTo.Rows.Add();
                            foldercount++;
                        }
                        else
                        {
                            newRow = true;
                        }
                        itemArray = new string[18 + highestlevel];
                        itemArray[catValIndex] += "\"";
                        itemArray[catHeadIndex] = (!string.IsNullOrEmpty(kvp.Value) ? kvp.Value : "NULL");
                    }
                    else
                    {
                        itemArray[catValIndex] += (!string.IsNullOrEmpty(kvp.Value) ? kvp.Value : "NULL") + ";";
                    }
                    if (catValuesKVP.IndexOf(kvp) == catValuesKVP.IndexOf(catValuesKVP.Last()))
                        itemArray[catValIndex] += "\"";
                    fileToWriteTo.Rows[foldercount].ItemArray = itemArray;
                }
                foldercount++;
            }
            catch (Exception e)
            {
                error = error + "\nError Message: " + e.Message + "\nStack Trace: " + e.StackTrace;
                throw e;
            }

        }

        private static List<string> ExtractPermission(CustomNode folder)
        {
            var permsList = new List<string>();

            var owner = "";
            var ownerGroup = "";
            var aclUsers = "";

            var addPermUsers = "";
            var deletePermUsers = "";
            var deleteVersionPermUsers = "";
            var editPermUsers = "";
            var editPermissionPermUsers = "";
            var modifyPermUsers = "";
            var reservePermUsers = "";
            var seeContentPermUsers = "";
            var seePermUsers = "";
            foreach (var permission in folder.Permissions)
            {
                if (permission != null)
                {
                    var name = "";
                    var type = permission.Type;

                    if (type.Equals("Group"))
                        name += "#";
                    name += permission.UserGroupName;

                    if (permission.Permissions[4] == 1)
                        if (string.IsNullOrEmpty(owner))
                            owner = permission.Owner;
                    if (type.Equals("Owner Group"))
                        ownerGroup = name;

                    if (type.Equals("User") || type.Equals("Group"))
                    {
                        if (string.IsNullOrEmpty(aclUsers))
                            aclUsers += name;
                        else
                            aclUsers += ";" + name;
                    }

                    if (permission.Permissions[4] == 1)
                    {
                        if (string.IsNullOrEmpty(addPermUsers))
                            addPermUsers = addPermUsers + name;
                        else
                            addPermUsers = addPermUsers + ";" + name;
                    }

                    if (permission.Permissions[7] == 1)
                    {
                        if (string.IsNullOrEmpty(deletePermUsers))
                            deletePermUsers = deletePermUsers + name;
                        else
                            deletePermUsers = deletePermUsers + ";" + name;
                    }

                    if (permission.Permissions[6] == 1)
                    {
                        if (string.IsNullOrEmpty(deleteVersionPermUsers))
                            deleteVersionPermUsers = deleteVersionPermUsers + name;
                        else
                            deleteVersionPermUsers = deleteVersionPermUsers + ";" + name;
                    }

                    if (permission.Permissions[3] == 1)
                    {
                        if (string.IsNullOrEmpty(editPermUsers))
                            editPermUsers = editPermUsers + name;
                        else
                            editPermUsers = editPermUsers + ";" + name;
                    }

                    if (permission.Permissions[8] == 1)
                    {
                        if (string.IsNullOrEmpty(editPermissionPermUsers))
                            editPermissionPermUsers = editPermissionPermUsers + name;
                        else
                            editPermissionPermUsers = editPermissionPermUsers + ";" + name;
                    }

                    if (permission.Permissions[2] == 1)
                    {
                        if (string.IsNullOrEmpty(modifyPermUsers))
                            modifyPermUsers = modifyPermUsers + name;
                        else
                            modifyPermUsers = modifyPermUsers + ";" + name;
                    }

                    if (permission.Permissions[5] == 1)
                    {
                        if (string.IsNullOrEmpty(reservePermUsers))
                            reservePermUsers = reservePermUsers + name;
                        else
                            reservePermUsers = reservePermUsers + ";" + name;
                    }

                    if (permission.Permissions[1] == 1)
                    {
                        if (string.IsNullOrEmpty(seeContentPermUsers))
                            seeContentPermUsers = seeContentPermUsers + name;
                        else
                            seeContentPermUsers = seeContentPermUsers + ";" + name;
                    }

                    if (permission.Permissions[0] == 1)
                    {
                        if (string.IsNullOrEmpty(seePermUsers))
                            seePermUsers = seePermUsers + name;
                        else
                            seePermUsers = seePermUsers + ";" + name;
                    }
                }
                else
                {
                    Trace.WriteLine("Permission not set correctly");
                }
            }
            permsList.Add(owner);
            permsList.Add(ownerGroup);
            permsList.Add(aclUsers);
            permsList.Add(seePermUsers);
            permsList.Add(seeContentPermUsers);
            permsList.Add(modifyPermUsers);
            permsList.Add(editPermUsers);
            permsList.Add(addPermUsers);
            permsList.Add(reservePermUsers);
            permsList.Add(deleteVersionPermUsers);
            permsList.Add(deletePermUsers);
            permsList.Add(editPermissionPermUsers);
            return permsList;
        }

        public static string ReadPassword()
        {
            var password = "";
            var info = Console.ReadKey(true);
            while (info.Key != ConsoleKey.Enter)
            {
                if (info.Key != ConsoleKey.Backspace)
                {
                    Console.Write("*");
                    password += info.KeyChar;
                }
                else if (info.Key == ConsoleKey.Backspace)
                {
                    if (!string.IsNullOrEmpty(password))
                    {
                        // remove one character from the list of password characters
                        password = password.Substring(0, password.Length - 1);
                        // get the location of the cursor
                        var pos = Console.CursorLeft;
                        // move the cursor to the left by one character
                        Console.SetCursorPosition(pos - 1, Console.CursorTop);
                        // replace it with space
                        Console.Write(" ");
                        // move the cursor to the left by one character again
                        Console.SetCursorPosition(pos - 1, Console.CursorTop);
                    }
                }

                info = Console.ReadKey(true);
            }

            // add a new line because user pressed enter at the end of their password
            Console.WriteLine();
            return password;
        }

        private static bool ConsoleCtrlCheck(CtrlTypes ctrlType)
        {
            var downloadDirectory = Path.Combine(Settings.Default.CsvOutput,
                StartTime.ToString("yyyyMMdd-HHmmss") + "_FilePlanFor " + parentNode.Name + ".csv");
            var longDownloadDirectory = Path.Combine(Settings.Default.CsvOutput, StartTime.ToString("yyyyMMdd-HHmmss") + "_PathTooLongFor" + parentNode.Name + ".csv");
            // Put your own handler here

            switch (ctrlType)

            {
                case CtrlTypes.CTRL_C_EVENT:
                    CsvWriter.ToCSV(filePlanDT, downloadDirectory);
                    CsvWriter.ToCSV(longPathFileDT, longDownloadDirectory);

                    break;


                case CtrlTypes.CTRL_BREAK_EVENT:
                    CsvWriter.ToCSV(filePlanDT, downloadDirectory);
                    CsvWriter.ToCSV(longPathFileDT, longDownloadDirectory);

                    break;


                case CtrlTypes.CTRL_CLOSE_EVENT:
                    CsvWriter.ToCSV(filePlanDT, downloadDirectory);
                    CsvWriter.ToCSV(longPathFileDT, longDownloadDirectory);

                    break;


                case CtrlTypes.CTRL_LOGOFF_EVENT:

                case CtrlTypes.CTRL_SHUTDOWN_EVENT:
                    CsvWriter.ToCSV(filePlanDT, downloadDirectory);
                    CsvWriter.ToCSV(longPathFileDT, longDownloadDirectory);

                    break;
            }

            return true;
        }

        #region unmanaged

        // Declare the SetConsoleCtrlHandler function

        // as external and receiving a delegate.


        [DllImport("Kernel32")]
        public static extern bool SetConsoleCtrlHandler(HandlerRoutine Handler, bool Add);


        // A delegate type to be used as the handler routine

        // for SetConsoleCtrlHandler.

        public delegate bool HandlerRoutine(CtrlTypes CtrlType);


        // An enumerated type for the control messages

        // sent to the handler routine.

        public enum CtrlTypes
        {
            CTRL_C_EVENT = 0,

            CTRL_BREAK_EVENT,

            CTRL_CLOSE_EVENT,

            CTRL_LOGOFF_EVENT = 5,

            CTRL_SHUTDOWN_EVENT
        }

        #endregion
    }
}