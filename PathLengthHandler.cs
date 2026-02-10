using MimeTypes;
using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace DownloadFilePlan
{
    public class PathLengthHandler
    {
        private readonly string BaseFilePath;
        private readonly string ShortBasePath;
        private readonly string LongBasePath;
        private static string DuplicateOTCSPath;
        private static string DuplicatePath;
        private static string FilePathToReplace;
        private readonly int PathMinLength;
        private readonly int PathMaxLength;
        private readonly Dictionary<long, string> NodePaths;
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        public PathLengthHandler(string baseFilePath)
        {
            BaseFilePath = baseFilePath;
            ShortBasePath = Properties.Settings.Default.LongFilePathLocation;
            LongBasePath = Properties.Settings.Default.SPOLongFilePathLocation;
            PathMinLength = Properties.Settings.Default.PathMinLength;
            PathMaxLength = Properties.Settings.Default.PathMaxLength;
            NodePaths = new Dictionary<long, string>();
            DuplicateOTCSPath = "";
            DuplicatePath = "";
            FilePathToReplace = "";
        }

        public (string path, bool isInputFolder, bool isSPOLong) ProcessFolderPath(CustomNode node)
        {
            bool isInputFolder = false;
            bool isSPOLong = false;
            string actualPath;

            // Build the full original path
            string[] pathParts = SplitPath(node.Path);
            string currentPath = BaseFilePath;
            for (int i = 0; i < pathParts.Length; i++)
            {
                var otcsPath = CleanFilePath(pathParts[i]);
                currentPath = Path.Combine(currentPath, otcsPath);
            }
            var uniqueInfo = GetUniqueFilePath(currentPath);
            currentPath = uniqueInfo.filePath;
            if (DuplicateOTCSPath != "")
            {
                if (node.Path.Contains(DuplicateOTCSPath))
                {
                    string newPath = currentPath.Replace(FilePathToReplace, DuplicatePath);
                    currentPath = newPath;
                }
            }
            if (uniqueInfo.isDuplicate == true)
            {
                DuplicateOTCSPath = node.Path;
                DuplicatePath = currentPath;

            }
            if (currentPath.Length > PathMinLength)
            {
                // Create path using node IDs to maintain hierarchy
                //actualPath = BuildHierarchicalNodePath(node);

                actualPath = currentPath;

                if (currentPath.Length < PathMaxLength)
                {
                    isInputFolder = true;
                }
                else
                {
                    isSPOLong = true;
                }
            }
            else
            {
                actualPath = currentPath;
            }

            // Store the path for this node ID for later use
            //NodePaths[node.Node.ID] = actualPath;

            return (actualPath, isInputFolder, isSPOLong);
        }

        private string BuildHierarchicalNodePath(CustomNode node)
        {
            var path = new System.Text.StringBuilder(ShortBasePath);

            // Build list of parent IDs
            var nodeHierarchy = new List<long>();
            var currentNode = node;

            // Start with the current node
            nodeHierarchy.Add(currentNode.Node.ID);

            // Get all parent IDs from the path
            string[] pathParts = node.Path.Split(new[] { '\\' }, StringSplitOptions.RemoveEmptyEntries);
            for (int i = pathParts.Length - 2; i >= 1; i--) // Skip the root "Test" folder
            {
                if (currentNode.Node.ParentID != 0)
                {
                    nodeHierarchy.Add(currentNode.Node.ParentID);
                    // Update current node's parent ID for next iteration
                    //currentNode.Node.ParentID = GetParentFromPath(pathParts[i], node.Node.ParentID);
                }
            }

            // Reverse the list to build path from root to leaf
            nodeHierarchy.Reverse();

            // Build the path
            foreach (var nodeId in nodeHierarchy)
            {
                path.Append(Path.DirectorySeparatorChar).Append(nodeId);
            }

            return path.ToString();
        }

        public (string path, bool isInputFile, bool isSPOLong) ProcessFilePath(string filePath, CustomNode node)
        {
            bool isInputFile = false;
            bool isSPOLong = false;
            string processedPath = filePath;

            if (DuplicateOTCSPath != "")
            {
                if (node.Path.Contains(DuplicateOTCSPath))
                {
                    string newPath = processedPath.Replace(FilePathToReplace, DuplicatePath);
                    processedPath = newPath;
                }
            }

            Logger.Info("file path from program logic: " + filePath);
            Logger.Info("long file path from app settings: " + ShortBasePath);

            if (filePath.Length > PathMinLength)
            {
                // Get the parent folder's processed path
                string parentPath;
                string extension = "";
                //if (NodePaths.TryGetValue(node.Node.ParentID, out string storedParentPath))
                //{
                //    parentPath = storedParentPath;
                //}
                //else
                //{
                //    // If parent path not found, use short base path
                //    parentPath = ShortBasePath;
                //}
                if (string.IsNullOrEmpty(GetFileExtension(CleanFileName(node.Node.Name))))
                {
                    if (!string.IsNullOrEmpty(node.MimeType))
                    {

                        // Check for special MIME types first
                        switch ((node.MimeType.ToLower()))
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
                                extension = MimeTypeMap.GetExtension(node.MimeType);
                                break;
                        }
                    }
                }
                else 
                {
                    extension = GetFileExtension(CleanFileName(node.Node.Name));

                    Logger.Info($"File {node.Node.ID} has {extension}");
                }

                if (filePath.Length < PathMaxLength)
                {
                    parentPath = ShortBasePath;
                    isInputFile = true;
                }
                else
                {
                    parentPath = LongBasePath;
                    isSPOLong = true;
                }
                Logger.Info("check parent path " + parentPath);
                string fileName = $"{node.Node.ID}{extension}";
                Logger.Info("check file id " + node.Node.ID);
                Logger.Info("check actual file name " + node.Node.Name);
                Logger.Info("check file name " + fileName);
                processedPath = Path.Combine(parentPath, fileName);
                
            }
            Logger.Info("check the processed path of a file: " + processedPath);
            return (processedPath, isInputFile, isSPOLong);
        }
        static string GetFileExtension(string fileName)
        {
            // Use Path.GetExtension to extract the extension
            string extension = Path.GetExtension(fileName);

            // If no extension was found or it's empty
            if (string.IsNullOrEmpty(extension))
            {
                return string.Empty;
            }

            // Check for numeric extensions like .00 in $120.00
            string extensionWithoutDot = extension.TrimStart('.');
            if (extensionWithoutDot.All(char.IsDigit))
            {
                return string.Empty;
            }

            // Validate if it's a proper extension (e.g., no spaces or invalid characters)
            if (extension.Contains(" ") || extension.Contains("(") || extension.Contains(")"))
            {
                return string.Empty; // Not a valid extension
            }

            return extension; // Path.GetExtension already includes the dot
        }
        private static string CleanFileName(string fileName)
        {
            // Get the last dot position to preserve extension
            int lastDotIndex = fileName.LastIndexOf('.');
            string nameWithoutExtension = lastDotIndex >= 0 ? fileName.Substring(0, lastDotIndex) : fileName;
            string extension = lastDotIndex >= 0 ? fileName.Substring(lastDotIndex) : "";

            // Pattern excludes the dot
            string pattern = @"[~#%&*{}\\\/:,<>?\|\""']";
            Regex regex = new Regex(pattern);

            // Clean only the filename part and then combine with extension
            return regex.Replace(nameWithoutExtension, "_") + extension;
        }
        private static (string filePath, bool isDuplicate) GetUniqueFilePath(string filePath)
        {
            if (!Directory.Exists(filePath))
                return (filePath, false);

            FilePathToReplace = filePath;
            string directory = Path.GetDirectoryName(filePath);
            string fileName = Path.GetFileNameWithoutExtension(filePath);
            string extension = Path.GetExtension(filePath);
            int counter = 1;

            while (Directory.Exists(filePath))
            {
                filePath = Path.Combine(directory, $"{fileName}_C{counter}{extension}");
                counter++;
            }

            return (filePath, true);
        }
        public static string CleanFilePath(string filePath)
        {
            // Pattern includes common illegal path characters
            string pattern = @"[~#%&*{}\\\/:,<>?\|\""']";
            Regex regex = new Regex(pattern);

            // Replace illegal characters with underscore
            return regex.Replace(filePath, "_");
        }
        public static string[] SplitPath(string path)
        {
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

                    // Check if this is a genuine path separator
                    bool isProbablySeparator = true;

                    // If this '\\' is not at the start or end of the string
                    if (i > 0 && i < path.Length - 1)
                    {
                        // Check for common operators that use '\\'
                        bool isPartOfOperator = false;

                        // Check for '>=' operator
                        if (i < path.Length - 1 && path[i + 1] == '=')
                        {
                            isPartOfOperator = true;
                        }

                        // Check for '<>' operator
                        if (i > 0 && path[i - 1] == '<')
                        {
                            isPartOfOperator = true;
                        }

                        // Look at previous char - if it's whitespace or part of an operator, this might not be a separator
                        bool hasPreviousContent = i > 0 && !char.IsWhiteSpace(path[i - 1]);
                        // Look at next char - if it's whitespace or part of an operator, this might not be a separator
                        bool hasNextContent = i < path.Length - 1 && !char.IsWhiteSpace(path[i + 1]);

                        // A real separator should have content on both sides and not be part of an operator
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

            // Add the last part
            if (startIndex < path.Length)
            {
                string finalSegment = path.Substring(startIndex).Trim();
                result.Add(finalSegment);
            }

            return result.ToArray();
        }
        private long GetParentFromPath(string pathPart, long currentParentId)
        {
            // This would need to be implemented based on your database lookup logic
            // Return the parent ID for the given path part
            return currentParentId;
        }
    }
}
