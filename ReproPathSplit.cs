using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Collections.Generic;

public class Program
{
    // Mock Settings
    static string SharePointURL = "sites/ssg-test/zTest";
    
    public static void Main()
    {
        Console.WriteLine("--- Test Case 1: Email Demo Split ---");
        TestEmailDemo();

        Console.WriteLine("\n--- Test Case 2: zTest Duplication ---");
        TestZTestDuplication();

        Console.WriteLine("\n--- Test Case 3: LF_1 Splitting ---");
        TestLF1Splitting();
    }

    static void TestEmailDemo()
    {
        string path = "Enterprise>zTest>DLFilePlan>Folder 1>Email Demo/\\_18112024_171540_1.msg";
        string fileName = "Email Demo_18112024_171540_1.msg"; // Cleaned filename usually
        
        string sanitizePathForLogs = SanitizePathForLogs(path, fileName);
        
        string destinationPath = "/" + SharePointURL.TrimStart('/') + 
                                (string.IsNullOrEmpty(sanitizePathForLogs) ? "" : "/" + sanitizePathForLogs.Replace('\\', '/').Trim('/')) + 
                                "/" + fileName;

        Console.WriteLine($"Path: {path}");
        Console.WriteLine($"FileName: {fileName}");
        Console.WriteLine($"Sanitized: {sanitizePathForLogs}");
        Console.WriteLine($"Destination: {destinationPath}");
        
        if (destinationPath.Contains("Email Demo/Email Demo"))
        {
            Console.WriteLine("FAIL: Email Demo folder detected.");
        }
        else
        {
            Console.WriteLine("PASS: Email Demo folder avoided.");
        }
    }

    static void TestZTestDuplication()
    {
        // Path starts with zTest (assuming relative to some root or just how it comes)
        // Adjusting path to match user's likely scenario where path contains zTest
        string path = "Enterprise>zTest>DLFilePlan>10-MB-Test.docx";
        string fileName = "10-MB-Test.docx";
        
        string sanitizePathForLogs = SanitizePathForLogs(path, fileName);
         
        string destinationPath = "/" + SharePointURL.TrimStart('/') + 
                                (string.IsNullOrEmpty(sanitizePathForLogs) ? "" : "/" + sanitizePathForLogs.Replace('\\', '/').Trim('/')) + 
                                "/" + fileName;

        Console.WriteLine($"Path: {path}");
        Console.WriteLine($"Sanitized: {sanitizePathForLogs}");
        Console.WriteLine($"Destination: {destinationPath}");

        if (destinationPath.Contains("zTest/zTest"))
        {
            Console.WriteLine("FAIL: zTest duplicated.");
        }
    }

    static void TestLF1Splitting()
    {
        // Guessing path structure for LF_1 based on user input
        // User said: /sites/ssg-test/zTest/DLFilePlan/LF_1/_W/O__/O/
        // Source: D:\zWork\Export\Downloaded_Items\zTest\DLFilePlan\LF_1__W_O__\
        // This implies the folder name itself might have special chars that SanitizePathForLogs treats as separators?
        
        string path = "Enterprise>zTest>DLFilePlan>LF_1/_W/O__/O"; // Hypothetical weird folder
        // Wait, SanitizePathForLogs replaces illegal chars.
        
        // If the NODE NAME in OTCS contains slashes?
        // e.g. "LF_1/_W/O_"
        
        string path2 = "Enterprise>zTest>DLFilePlan>LF_1/_W/O__>SomeFile.txt";
        string fileName = "SomeFile.txt";

        string sanitizePathForLogs = SanitizePathForLogs(path2, fileName);
        Console.WriteLine($"Path: {path2}");
        Console.WriteLine($"Sanitized: {sanitizePathForLogs}");
        
        // SanitizePathForLogs splits by > then calls CleanFilePath on segments.
        // CleanFilePath replaces illegal chars with _.
        // BUT SanitizePathForLogs joins with '/'. 
        
        // If SplitPath handles mixed separators?
    }

    // --- COPIED CODE ---

    public static string SanitizePathForLogs(string path, string fileNameWithExt = "")
    {
        if (string.IsNullOrWhiteSpace(path))
            return string.Empty;

        string[] pathParts = SplitPath(path);
        // Console.WriteLine("DEBUG: Segments: " + string.Join(", ", pathParts));

        if (pathParts.Length == 0)
            return string.Empty;

        int folderCount = pathParts.Length;
        
        if (!string.IsNullOrEmpty(fileNameWithExt))
        {
            string cleanedFileName = CleanFileName(fileNameWithExt);
            string fileNameBase = Path.GetFileNameWithoutExtension(cleanedFileName);
            string fileNameExt = Path.GetExtension(cleanedFileName);
            string lastSegment = pathParts[pathParts.Length - 1];
            
            string lastSegmentFileName = lastSegment;
            int lastSlash = Math.Max(lastSegment.LastIndexOf('/'), lastSegment.LastIndexOf('\\'));
            if (lastSlash >= 0 && lastSlash < lastSegment.Length - 1)
            {
                lastSegmentFileName = lastSegment.Substring(lastSlash + 1);
            }
            
            string lastSegmentCleaned = CleanFileName(lastSegmentFileName);
            string lastSegmentBase = Path.GetFileNameWithoutExtension(lastSegmentCleaned);
            
            // PROPOSED FIX INSERTED HERE FOR TESTING
            string normLast = Regex.Replace(lastSegment.Replace('/', '_').Replace('\\', '_'), "_+", "_");
            string normFile = Regex.Replace(cleanedFileName, "_+", "_");

            bool isDocumentName = 
                lastSegment.Contains(cleanedFileName) ||  
                lastSegmentFileName.Contains(fileNameBase) ||  
                lastSegmentCleaned.Equals(cleanedFileName, StringComparison.OrdinalIgnoreCase) ||  
                (lastSegmentBase.Equals(fileNameBase, StringComparison.OrdinalIgnoreCase) && 
                 Path.GetExtension(lastSegmentCleaned).Equals(fileNameExt, StringComparison.OrdinalIgnoreCase));
            
            // Check normalized match
            
            if (normLast.Equals(normFile, StringComparison.OrdinalIgnoreCase) || 
                normFile.Contains(normLast) || 
                normLast.Contains(normFile)) // Contains might be risky if file name is short?
            {
                isDocumentName = true;
            }

            if (isDocumentName)
            {
                folderCount = pathParts.Length - 1; 
            }
        }
        else
        {
            if (pathParts.Length > 1)
            {
                string lastSegment = pathParts[pathParts.Length - 1];
                if (lastSegment.Contains('.') && (lastSegment.Contains('/') || lastSegment.Contains('\\')))
                {
                    folderCount = pathParts.Length - 1; 
                }
            }
        }

        var folderSegments = pathParts.Take(folderCount)
            .Select(s => CleanFilePath(s))
            .Where(s => !string.IsNullOrEmpty(s));
        
        return string.Join("/", folderSegments);
    }

    public static string[] SplitPath(string path)
    {
        if (path.IndexOf('>') >= 0)
        {
            var segments = path.Split(new[] { '>' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(s => s.Trim())
                .Where(s => !string.IsNullOrEmpty(s))
                .ToArray();
            return segments;
        }
        return path.Split(new[] { '\\' }, StringSplitOptions.RemoveEmptyEntries);
    }

    static string CleanFileName(string fileName)
    {
        if (string.IsNullOrWhiteSpace(fileName))
            return string.Empty;

        var normalizedName = fileName.Replace('\\', '/');
        int lastSeparator = normalizedName.LastIndexOf('/');
        if (lastSeparator >= 0 && lastSeparator < normalizedName.Length - 1)
        {
            normalizedName = normalizedName.Substring(lastSeparator + 1);
        }

        string extension = Path.GetExtension(normalizedName); 
        string nameWithoutExtension = extension.Length > 0
            ? normalizedName.Substring(0, normalizedName.Length - extension.Length)
            : normalizedName;

        string pattern = @"[~#%&*{}\\/:,<>?|\""']";
        var regex = new Regex(pattern);

        var cleaned = regex.Replace(nameWithoutExtension.Trim(), "_");

        cleaned = Regex.Replace(cleaned, "_+", "_").Trim('_', ' ');

        return cleaned + extension;
    }

    public static string CleanFilePath(string filePath)
    {
        string pattern = @"[~#%&*{,}<>?|\""']"; 
        Regex regex = new Regex(pattern);
        return regex.Replace(filePath, "_");
    }
}
