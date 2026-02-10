using DownloadFilePlan.Properties;
using NLog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web.UI;

namespace DownloadFilePlan
{
    class DBAccess
    {
        private static Dictionary<long, User> users = new Dictionary<long, User>();
        private static Tuple<string, string> dbAuthInfo = new Tuple<string, string>("", "");
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        public static void SetAuthInfo(string user, string pass)
        {
            dbAuthInfo = new Tuple<string, string>(user, pass);
        }

        private static string GetConnectionString()
        {
            var dbDataSource = Properties.Settings.Default.DBSource;
            var dbInitialCatalog = Properties.Settings.Default.DBCatalog;

            return @"Data Source=" + dbDataSource + ";Initial Catalog=" + dbInitialCatalog + "; User ID=" + dbAuthInfo.Item1 + ";Password=" + dbAuthInfo.Item2 + ";Connection Timeout=" + 60 + ";"; ;
        }

        public static bool ValidateConnection()
        {
            string connectionString = GetConnectionString();
            string query = @"SELECT DataID FROM otcs.DTree WHERE DataID = 2000";
            string value = "";
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    SqlCommand cmd = new SqlCommand(query, connection);
                    connection.Open();

                    SqlDataReader reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        value = reader["DataID"].ToString();
                    }

                    connection.Close();
                }
            } catch(Exception ex)
            {
                Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|" + ex.Message);
                return false;
            }

            return value == "2000";
        }

        public static bool DoesPathFuncExist()
        {
            string connectionString = GetConnectionString();
            long nodeId = Properties.Settings.Default.TargetedNode;
            string query = $"SELECT dbo.fn_llpathwsg({nodeId}) AS 'Path'";
            string value = "";
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    SqlCommand cmd = new SqlCommand(query, connection);
                    connection.Open();

                    SqlDataReader reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        value = reader["Path"].ToString();
                    }

                    connection.Close();
                }
            }
            catch (Exception ex)
            {
                return false;
            }

            return !string.IsNullOrEmpty(value);
        }

        public static CustomNode  GetNodeById(long id)
        {
            var table = new DataTable();
            using (SqlConnection conn = new SqlConnection(GetConnectionString()))
            {
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandTimeout = 480;
                    var query =
                        @"SELECT dt.DataID, dt.Name, dt.ParentID, dt.DComment, dt.SubType, dt.VersionNum, dt.CreateDate, dt.ModifyDate, 
                               kc.Name AS CreatedBy, km.Name AS ModifiedBy,
                               dbo.fn_llpathwsg(dt.DataID) AS 'Path',
                               v.MimeType
                        FROM otcs.DTree dt
                        LEFT JOIN otcs.KUAF kc ON kc.ID = dt.CreatedBy
                        LEFT JOIN otcs.KUAF km ON km.ID = dt.ModifiedBy
                        LEFT JOIN (
                            SELECT DocID, MimeType
                            FROM otcs.DVersData 
                            WHERE (VerType IS NULL OR VerType != 'otthumb')
                            AND Version = (
                                SELECT MAX(Version) 
                                FROM otcs.DVersData v2 
                                WHERE v2.DocID = DVersData.DocID 
                                AND (v2.VerType IS NULL OR v2.VerType != 'otthumb')
                            )
                        ) v ON v.DocID = dt.DataID
                        WHERE dt.DataID = @NodeId
                        AND dt.Deleted = 0 
                        AND dt.Catalog != 2";

                    cmd.CommandText = query;
                    cmd.Parameters.Add(new SqlParameter("NodeId", id));

                    var adapter = new SqlDataAdapter();
                    adapter.SelectCommand = cmd;
                    adapter.Fill(table);
                }

                conn.Close();
            }

            if (table.Rows.Count > 0)
            {
                var row = table.Rows[0];
                return ExtractNodeFromRow(row);
            }

            return new CustomNode { Node = new Node { ID = id } };
        }

        public static CustomNode GetNodeByID(long id)
        {
            var table = new DataTable();
            using (SqlConnection conn = new SqlConnection(GetConnectionString()))
            {
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandTimeout = 480;
                    var query =
                        @"SELECT dt.DataID, dt.Name, dt.ParentID, dt.DComment, dt.SubType, dt.VersionNum, dt.CreateDate, dt.ModifyDate, 
                               kc.Name AS CreatedBy, km.Name AS ModifiedBy,
                               dbo.fn_llpathwsg(dt.DataID) AS 'Path',
                               v.MimeType
                        FROM otcs.DTree dt
                        LEFT JOIN otcs.KUAF kc ON kc.ID = dt.CreatedBy
                        LEFT JOIN otcs.KUAF km ON km.ID = dt.ModifiedBy
                        LEFT JOIN (
                            SELECT DocID, MimeType
                            FROM otcs.DVersData 
                            WHERE (VerType IS NULL OR VerType != 'otthumb')
                            AND Version = (
                                SELECT MAX(Version) 
                                FROM otcs.DVersData v2 
                                WHERE v2.DocID = DVersData.DocID 
                                AND (v2.VerType IS NULL OR v2.VerType != 'otthumb')
                            )
                        ) v ON v.DocID = dt.DataID
                        WHERE dt.DataID = @NodeId
                        AND dt.Deleted = 0 
                        AND dt.Catalog != 2";

                    cmd.CommandText = query;
                    cmd.Parameters.Add(new SqlParameter("NodeId", id));

                    var adapter = new SqlDataAdapter();
                    adapter.SelectCommand = cmd;
                    adapter.Fill(table);
                }

                conn.Close();
            }

            if (table.Rows.Count > 0)
            {
                var row = table.Rows[0];
                return ExtractNodeFromRow(row);
            }

            return new CustomNode { Node = new Node { ID = id } };
        }

        public static CustomNode GetNodeByIds(long id)
        {
            var table = new DataTable();
            using (SqlConnection conn = new SqlConnection(GetConnectionString()))
            {
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandTimeout = 480;
                    var query =
                        @"SELECT dt.DataID, dt.Name, dt.ParentID, dt.DComment, dt.SubType, dt.VersionNum, dt.CreateDate, dt.ModifyDate, 
                               kc.Name AS CreatedBy, km.Name AS ModifiedBy,
                               dbo.fn_llpathwsg(dt.DataID) AS 'Path',
                               v.MimeType
                        FROM otcs.DTree dt
                        LEFT JOIN otcs.KUAF kc ON kc.ID = dt.CreatedBy
                        LEFT JOIN otcs.KUAF km ON km.ID = dt.ModifiedBy
                        LEFT JOIN (
                            SELECT DocID, MimeType
                            FROM otcs.DVersData 
                            WHERE (VerType IS NULL OR VerType != 'otthumb')
                            AND Version = (
                                SELECT MAX(Version) 
                                FROM otcs.DVersData v2 
                                WHERE v2.DocID = DVersData.DocID 
                                AND (v2.VerType IS NULL OR v2.VerType != 'otthumb')
                            )
                        ) v ON v.DocID = dt.DataID
                        WHERE dt.DataID = @NodeId
                        AND dt.Deleted = 0 
                        AND dt.Catalog != 2";

                    cmd.CommandText = query;
                    cmd.Parameters.Add(new SqlParameter("NodeId", id));

                    var adapter = new SqlDataAdapter();
                    adapter.SelectCommand = cmd;
                    adapter.Fill(table);
                }

                conn.Close();
            }

            if (table.Rows.Count > 0)
            {
                var row = table.Rows[0];
                return ExtractNodeFromRow(row);
            }

            return new CustomNode { Node = new Node { ID = id } };
        }

        public static List<CustomNode> GetAllFoldersAndFiles(long id, ref int highestLevel) // add to prompt to either copy attributes&permissions
        {
            var levelAndNode = new Dictionary<int, List<CustomNode>>();
            var initial = 0;
            var rootNode = new CustomNode();
            int logEvery = Properties.Settings.Default.LogEvery;

            rootNode.Node.ID = id;

            var rootNodeList = new List<CustomNode>();
            rootNodeList.Add(rootNode);
            levelAndNode.Add(initial, rootNodeList);

            var nodes = new List<CustomNode>();
            var table = new DataTable();
            Logger.Info("Getting nodes...");

            using(SqlConnection conn = new SqlConnection(GetConnectionString()))
            {
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandTimeout = 480;
                    var query =
                        @"SELECT dt.DataID, dt.Name, dt.ParentID, dt.DComment, dt.SubType, dt.VersionNum, dt.CreateDate, dt.ModifyDate, 
                               kc.Name AS CreatedBy, km.Name AS ModifiedBy,
                               dbo.fn_llpathwsg(dt.DataID) AS 'Path',
                               v.MimeType
                        FROM otcs.DTree dt
                        INNER JOIN otcs.DTreeAncestors anc ON anc.DataID = dt.DataID
                        LEFT JOIN otcs.KUAF kc ON kc.ID = dt.CreatedBy
						LEFT JOIN otcs.KUAF km ON km.ID = dt.ModifiedBy
                        LEFT JOIN (
                            SELECT DocID, MimeType
                            FROM otcs.DVersData 
                            WHERE (VerType IS NULL OR VerType != 'otthumb')
                            AND Version = (
                                SELECT MAX(Version) 
                                FROM otcs.DVersData v2 
                                WHERE v2.DocID = DVersData.DocID 
                                AND (v2.VerType IS NULL OR v2.VerType != 'otthumb')
                            )
                        ) v ON v.DocID = dt.DataID
                        WHERE anc.AncestorID = @RootNode
                        AND dt.DataID != @RootNode
                        AND dt.Deleted = 0 
                        AND dt.Catalog != 2
                        ORDER BY Path";

                    cmd.CommandText = query;
                    cmd.Parameters.Add(new SqlParameter("RootNode", id));

                    var adapter = new SqlDataAdapter();
                    adapter.SelectCommand = cmd;
                    adapter.Fill(table);
                }

                conn.Close();
            }

            var last = table.Rows[table.Rows.Count - 1];
            var nodesDictionary = new Dictionary<long, CustomNode>();

            Stopwatch sw = new Stopwatch();
            sw.Start();
            var est = 0.0;

            //Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Extracting " + table.Rows.Count + " Nodes");
            //Logger.Info($"Extracting {table.Rows.Count} nodes");
            for (int i = 0; i < table.Rows.Count; i++)
            {
                if (i % logEvery == 0)
                {
                    est = (table.Rows.Count - i) * (sw.ElapsedMilliseconds / 10 != 0 ? sw.ElapsedMilliseconds / 10 : 1);
                    //Logger.Info($"Extracting node {(i + 1)} out of {table.Rows.Count} with estimated completion time: {Math.Round(est / 1000 / 60 / 60 / 60 / 60, 4)} seconds");
                    sw.Restart();
                }

                var row = table.Rows[i];
                var thisNode = ExtractNodeFromRow(row);
                try
                {
                    nodesDictionary.Add(long.Parse(row["DataID"].ToString()), thisNode);
                } 
                catch (Exception ex)
                {
                    Logger.Info(ex.Message);
                    Logger.Trace(ex.StackTrace);
                }
            }

            nodes = nodesDictionary.Values.ToList();

            sw.Stop();
            Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Sorting Nodes");
            Logger.Info("Sorting nodes...");
            parse(id, nodes, ref levelAndNode, initial);
            highestLevel = levelAndNode.Count;

            return nodes;
        }

        public static Dictionary<long, User> GetAllUsers()
        {
            var table = new DataTable();
            var usersDictionary = new Dictionary<long, User>();
            int logEvery = Properties.Settings.Default.LogEvery;
            Stopwatch sw = new Stopwatch();
            var est = 0.0;
            //Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Getting Users...");

            using (SqlConnection conn = new SqlConnection(GetConnectionString()))
            {
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandTimeout = 480;
                    var query = @"SELECT ID, Name, FirstName, LastName FROM otcs.KUAF WHERE Type = 0";
                    cmd.CommandText = query;

                    table = new DataTable();

                    SqlDataAdapter adapter = new SqlDataAdapter();

                    adapter.SelectCommand = cmd;
                    adapter.Fill(table);
                }

                conn.Close();
            }

            //Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Extracting " + table.Rows.Count + " Users");
            for (int i = 0; i < table.Rows.Count; i++)
            {
                if (i % logEvery == 0)
                {
                    est = (table.Rows.Count - i) * (sw.ElapsedMilliseconds / 10 != 0 ? sw.ElapsedMilliseconds / 10 : 1);
                    sw.Restart();
                    //Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Extracting User " + (i + 1) + " out of " + table.Rows.Count + " Estimated Completion Time: " + Math.Round(est / 1000 / 60 / 60, 4) + " Hours");
                }

                var row = table.Rows[i];
                var thisUser = ExtractUserFromRow(row);
                usersDictionary.Add(thisUser.ID, thisUser);
            }

            return usersDictionary;
        }

        private static User ExtractUserFromRow(DataRow row)
        {
            User user = new User();

            user.ID = long.Parse(row["ID"].ToString());

            if (row["Name"].GetType() == typeof(DBNull)) {
                user.Name = "";
            } else
            {
                user.Name = row["Name"].ToString();
            }

            if (row["FirstName"].GetType() == typeof(DBNull))
            {
                user.FirstName = "";
            } else
            {
                user.FirstName = row["FirstName"].ToString();
            }

            if (row["LastName"].GetType() == typeof(DBNull))
            {
                user.LastName = "";
            } else
            {
                user.LastName = row["LastName"].ToString();
            }

            return user;
        }

        private static CustomPermissions ExtractPermissionsFromRow(DataRow row)
        {
            CustomPermissions permissions = new CustomPermissions();

            permissions.NodeID = long.Parse(row["DataID"].ToString());
            permissions.RightID = long.Parse(row["RightID"].ToString());

            if (row["Owner"].GetType() != typeof(DBNull) && !String.IsNullOrEmpty(row["Owner"].ToString()))
            {
                permissions.Owner = row["Owner"].ToString();
            } else if (row["OwnerLoginName"].GetType() != typeof(DBNull))
            {
                permissions.Owner = row["OwnerLoginName"].ToString();
            } else
            {
                permissions.Owner = "UNK";
            }

            permissions.UserGroupName = row["UserGroupName"].ToString();
            permissions.Type = row["Type"].ToString();

            permissions.Permissions[0] = Convert.ToInt64(row["See"].ToString());
            permissions.Permissions[1] = Convert.ToInt64(row["SeeContents"].ToString());
            permissions.Permissions[2] = Convert.ToInt64(row["ModifyPerm"].ToString());
            permissions.Permissions[3] = Convert.ToInt64(row["EditAttributes"].ToString());
            permissions.Permissions[4] = Convert.ToInt64(row["AddItems"].ToString()); 
            permissions.Permissions[5] = Convert.ToInt64(row["Reserve"].ToString());
            permissions.Permissions[6] = Convert.ToInt64(row["DeleteVersions"].ToString());
            permissions.Permissions[7] = Convert.ToInt64(row["DeletePerm"].ToString());
            permissions.Permissions[8] = Convert.ToInt64(row["EditPermissions"].ToString());

            return permissions;
        }

        private static CustomNode ExtractNodeFromRow(DataRow row)
        {
            CustomNode thisNode = new CustomNode();
            thisNode.Node.ID = long.Parse(row["DataID"].ToString());
            thisNode.Node.ParentID = long.Parse(row["ParentID"].ToString());
            thisNode.Node.Name = Regex.Replace((row["Name"].ToString()), @"\r\n?|\n", " ");
            if (row["DComment"].GetType() != typeof(DBNull))
            {
                thisNode.Node.Comment = Regex.Replace((row["DComment"].ToString()), @"\r\n?|\n", " ");
            }

            thisNode.Node.DisplayType = row["SubType"].ToString();
            thisNode.Version = long.Parse(row["VersionNum"].ToString());
            thisNode.MimeType = row["MimeType"].ToString();
            thisNode.Path = row["Path"].ToString();

            if (row.Table.Columns.Contains("CreateDate") && row["CreateDate"] != DBNull.Value)
            {
                thisNode.Node.CreationDate = (DateTime)row["CreateDate"];
            }
            if (row.Table.Columns.Contains("ModifyDate") && row["ModifyDate"] != DBNull.Value)
            {
                thisNode.Node.ModifiedDate = (DateTime)row["ModifyDate"];
            }
            thisNode.Node.CreatedBy = row["CreatedBy"].ToString();
            thisNode.Node.ModifiedBy = row["ModifiedBy"].ToString();
            //thisNode.Path = NodesAPI.GetPath(long.Parse(row["DataID"].ToString()));
            return thisNode;
        }

        private static CustomNode ExtractCategoryFromRow(DataRow row)
        {
            CustomNode thisNode = new CustomNode();

            thisNode.Node.ID = long.Parse(row["DataID"].ToString());
            KeyValuePair<int, string> catValuesKVP = new KeyValuePair<int, string>();
            catValuesKVP = ExtractInfoFromRows(row);
            thisNode.CatValuesKvp = catValuesKVP;
            return thisNode;
        }

        private static KeyValuePair<int, string> ExtractInfoFromRows(DataRow row)
        {
            return ExtractCategoriesIntoString(row);
        }

        private static KeyValuePair<int, string> ExtractCategoriesIntoString(DataRow row) {
            var key = 0;
            var value = "";
            long attrType = long.Parse(row["AttrType"].ToString());

            try
            {
                key = Convert.ToInt32(row["AttrID"].ToString());

                if (attrType == -18)
                {
                    var cat = "";

                    if (!String.IsNullOrEmpty(cat = (row["Cat"].GetType() == typeof(DBNull) ? "" : row["Cat"].ToString())))
                    {
                        value = cat;
                    }
                    else
                    {
                        value = "";
                    }
                }
                else if (!String.IsNullOrEmpty((row["AttrName"].GetType() == typeof(DBNull) ? "" : row["AttrName"].ToString())))
                {
                    var cat = (row["Cat"].GetType() == typeof(DBNull) ? "" : row["Cat"].ToString());
                    value = (row["AttrName"].GetType() == typeof(DBNull) ? "" : row["AttrName"].ToString()) + ":";
                    if (attrType == 14)
                    {
                        var user = users[Convert.ToInt32(cat)];
                        cat = "";
                        if (!String.IsNullOrWhiteSpace(user.Name))
                        {
                            cat = user.Name;
                        }
                        else if (!String.IsNullOrWhiteSpace(user.FirstName))
                        {
                            cat = user.FirstName;
                            if (!String.IsNullOrWhiteSpace(user.LastName))
                            {
                                cat += user.LastName;
                            }
                        }   
                    }

                    value += cat;
                }
                else
                {
                    value = "";
                }
                } catch(Exception) { }

            var kvp = new KeyValuePair<int, string>(key, value);
            return kvp;
        }
        public static void parse(long root, List<CustomNode> allNodes, ref Dictionary<int, List<CustomNode>> dictionary, int level)
        {
            level++;

            var thisNode = allNodes.Find(delegate (CustomNode nd) { return nd.Node.ID == root; });
            var theseNodes = allNodes.FindAll(delegate (CustomNode nd) { return nd.Node.ParentID == root; });

            if (dictionary.ContainsKey(level))
            {
                foreach(var node in theseNodes)
                {
                    dictionary[level].Add(node);
                }
            } else
            {
                dictionary.Add(level, theseNodes);
            }

            foreach(var nodeWithCat in theseNodes)
            {
                if (nodeWithCat.Node.DisplayType == "0" || nodeWithCat.Node.DisplayType == "751")
                {
                    parse(nodeWithCat.Node.ID, allNodes, ref dictionary, level);
                }
            }
        }
    }
}
