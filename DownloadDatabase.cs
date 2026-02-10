using DownloadFilePlan.Models;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Threading;

namespace DownloadFilePlan
{
    public class DownloadDatabase
    {
        private readonly string _connectionString;
        private static readonly int _maxRetries = 3;
        private static readonly int _retryDelayMs = 100;

        public DownloadDatabase(string dbPath)
        {
            _connectionString = $"Data Source={dbPath};Version=3;";
            InitializeDatabase();
        }

        private T ExecuteWithRetry<T>(Func<SQLiteConnection, T> operation)
        {
            for (int attempt = 1; attempt <= _maxRetries; attempt++)
            {
                try
                {
                    using (var connection = new SQLiteConnection(_connectionString))
                    {
                        connection.Open();
                        return operation(connection);
                    }
                }
                catch (SQLiteException ex) when (ex.Message.Contains("database is locked") && attempt < _maxRetries)
                {
                    Thread.Sleep(_retryDelayMs * attempt); // Exponential backoff
                }
            }
            throw new Exception($"Failed to execute database operation after {_maxRetries} attempts due to database locks");
        }

        private void ExecuteWithRetry(Action<SQLiteConnection> operation)
        {
            ExecuteWithRetry<object>(connection =>
            {
                operation(connection);
                return null;
            });
        }

        private void InitializeDatabase()
        {
            ExecuteWithRetry(connection =>
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        CREATE TABLE IF NOT EXISTS DownloadSession (
                            Id INTEGER PRIMARY KEY CHECK (Id = 1),
                            FilePath TEXT,
                            CSVFilePath TEXT,
                            EncryptedDownloads INTEGER,
                            CSPKeyName TEXT
                        );

                        CREATE TABLE IF NOT EXISTS DownloadRecords (
                            Id INTEGER PRIMARY KEY,
                            ParentId INTEGER,
                            Name TEXT,
                            DisplayType TEXT,
                            OTCSPath TEXT,
                            LocalPath TEXT,
                            Version INTEGER,
                            Downloaded INTEGER,
                            TotalBytes INTEGER,
                            DownloadedBytes INTEGER
                        )";
                    command.ExecuteNonQuery();
                }
            });
        }

        public void InsertOrUpdateRecord(DownloadRecord record)
        {
            ExecuteWithRetry(connection =>
            {
                using (var transaction = connection.BeginTransaction())
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        INSERT OR IGNORE INTO DownloadRecords 
                        (Id, ParentId, Name, DisplayType, OTCSPath, LocalPath, Version, Downloaded)
                        VALUES (@id, @parentId, @name, @displayType, @otcspath, @localpath, @version, @downloaded)";

                    command.Parameters.AddWithValue("@id", record.Id);
                    command.Parameters.AddWithValue("@parentId", record.ParentId);
                    command.Parameters.AddWithValue("@name", record.Name);
                    command.Parameters.AddWithValue("@displayType", record.DisplayType);
                    command.Parameters.AddWithValue("@otcspath", record.OTCSPath);
                    command.Parameters.AddWithValue("@localpath", record.LocalPath);
                    command.Parameters.AddWithValue("@version", record.Version);
                    command.Parameters.AddWithValue("@downloaded", record.Downloaded ? 1 : 0);

                    command.ExecuteNonQuery();
                    transaction.Commit();
                }
            });
        }

        public DownloadRecord GetDownloadRecord(long id)
        {
            return ExecuteWithRetry(connection =>
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT * FROM DownloadRecords WHERE Id = @id";
                    command.Parameters.AddWithValue("@id", id);

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return new DownloadRecord
                            {
                                Id = Convert.ToInt64(reader["Id"]),
                                ParentId = Convert.ToInt64(reader["ParentId"]),
                                Name = reader["Name"].ToString(),
                                DisplayType = reader["DisplayType"].ToString(),
                                OTCSPath = reader["OTCSPath"].ToString(),
                                Version = Convert.ToInt32(reader["Version"]),
                                Downloaded = Convert.ToInt32(reader["Downloaded"]) != 0
                            };
                        }
                        return null;
                    }
                }
            });
        }

        public void SetDownloadSession(DownloadSession session)
        {
            ExecuteWithRetry(connection =>
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        INSERT OR REPLACE INTO DownloadSession 
                        (Id, FilePath, CSVFilePath, EncryptedDownloads, CSPKeyName)
                        VALUES (1, @filePath, @csvFilePath, @encryptedDownloads, @cspKeyName)";

                    command.Parameters.AddWithValue("@filePath", session.FilePath);
                    command.Parameters.AddWithValue("@csvFilePath", session.CSVFilePath);
                    command.Parameters.AddWithValue("@encryptedDownloads", session.EncryptedDownloads ? 1 : 0);
                    command.Parameters.AddWithValue("@cspKeyName", session.CSPKeyName);

                    command.ExecuteNonQuery();
                }
            });
        }

        public DownloadSession GetDownloadSession()
        {
            return ExecuteWithRetry(connection =>
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT * FROM DownloadSession WHERE Id = 1";

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return new DownloadSession
                            {
                                FilePath = reader["FilePath"].ToString(),
                                CSVFilePath = reader["CSVFilePath"].ToString(),
                                EncryptedDownloads = Convert.ToInt32(reader["EncryptedDownloads"]) != 0,
                                CSPKeyName = reader["CSPKeyName"].ToString()
                            };
                        }
                        return null;
                    }
                }
            });
        }

        public void ClearDownloadRecords()
        {
            ExecuteWithRetry(connection =>
            {
                using (var transaction = connection.BeginTransaction())
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "DELETE FROM DownloadRecords";
                    command.ExecuteNonQuery();
                    transaction.Commit();
                }
            });
        }

        public bool SessionExists()
        {
            return ExecuteWithRetry(connection =>
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT COUNT(*) FROM DownloadSession";
                    return Convert.ToInt32(command.ExecuteScalar()) > 0;
                }
            });
        }

        public void DeleteSession()
        {
            ExecuteWithRetry(connection =>
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "DELETE FROM DownloadSession";
                    command.ExecuteNonQuery();
                }
            });
        }

        public long GetLastDownloadedRecordId()
        {
            return ExecuteWithRetry(connection =>
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT MAX(Id) FROM DownloadRecords WHERE Downloaded = 1";
                    var result = command.ExecuteScalar();
                    return result == DBNull.Value ? 0 : Convert.ToInt64(result);
                }
            });
        }

        public long GetLastDownloadedFolderId()
        {
            return ExecuteWithRetry(connection =>
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT MAX(Id) FROM DownloadRecords WHERE Downloaded = 1 AND DisplayType = '0'";
                    var result = command.ExecuteScalar();
                    return result == DBNull.Value ? 0 : Convert.ToInt64(result);
                }
            });
        }

        public long GetLastDownloadedDocumentId()
        {
            return ExecuteWithRetry(connection =>
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT MAX(Id) FROM DownloadRecords WHERE Downloaded = 1 AND DisplayType IN ('144', '749')";
                    var result = command.ExecuteScalar();
                    return result == DBNull.Value ? 0 : Convert.ToInt64(result);
                }
            });
        }

        public bool DoesLocalPathExist(string folderPath)
        {
            return ExecuteWithRetry(connection =>
            {
                using (var command = new SQLiteCommand(@"
                    SELECT 1 
                    FROM DownloadRecords 
                    WHERE LocalPath = @Path 
                    AND Version != 0 
                    LIMIT 1", connection))
                {
                    command.Parameters.AddWithValue("@Path", folderPath);
                    var result = command.ExecuteScalar();
                    return result != null;
                }
            });
        }

        public long GetFolderIdByPath(string folderPath)
        {
            return ExecuteWithRetry(connection =>
            {
                using (var command = new SQLiteCommand(@"
                    SELECT Id 
                    FROM DownloadRecords 
                    WHERE OTCSPath = @Path 
                    AND DisplayType IN ('0', '141')", connection))
                {
                    command.Parameters.AddWithValue("@Path", folderPath);
                    var result = command.ExecuteScalar();
                    return result != null ? Convert.ToInt64(result) : 0;
                }
            });
        }

        public List<long> GetAllDownloadedIds()
        {
            return ExecuteWithRetry(connection =>
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT Id FROM DownloadRecords WHERE Downloaded = 1";
                    var ids = new List<long>();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            ids.Add(reader.GetInt64(0));
                        }
                    }
                    return ids;
                }
            });
        }
    }
}