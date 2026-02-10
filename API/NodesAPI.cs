using Newtonsoft.Json.Linq;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DownloadFilePlan
{
    class NodesAPI
    {

        public static async Task<Dictionary<string, string>> GetCategoryValue(long id)
        {
            RestClient client = new RestClient(Properties.Settings.Default.APIUrl + "v1/forms/nodes/update");
            RestRequest request = new RestRequest("", Method.Get);
            request.AddParameter("id", id);

            try
            {
                string otcsTicket = await AuthAPI.AuthenticateUser();
                if (String.IsNullOrEmpty(otcsTicket))
                {
                    throw new Exception("Invalid otcsTicket");
                }

                request.AddHeader("OTCSTicket", otcsTicket);

                RestResponse response = await client.ExecuteAsync(request);
                //Console.WriteLine(response.Content.ToString());
                response.ThrowIfError();

                if (!response.IsSuccessful)
                {
                    throw new Exception("v1/forms/nodes/update/?id is not succesful");
                }

                var content = JObject.Parse(response.Content);
                var forms = content["forms"];
                foreach (var form in forms)
                {
                    if (form is JObject formObj && formObj["role_name"]?.Value<string>() == "categories")
                    {
                        var result = await ParseMappedFields(form.ToString());
                        return result;
                    }
                }
                return new Dictionary<string, string>();

            } catch (Exception ex)
            {
                Trace.WriteLine("_GetCategoryByNodeID failed");
                Trace.WriteLine(ex.StackTrace);
                throw;
            } finally
            {
                client.Dispose();
            }
        }
        public static async Task<Dictionary<string, string>> ParseMappedFields(string json)
        {
            var result = new Dictionary<string, string>();
            var jObj = JObject.Parse(json);

            var data = jObj["data"] as JObject;
            var options = jObj["options"]?["fields"] as JObject;
            var schema = jObj["schema"]?["properties"] as JObject;

            if (data == null || options == null || schema == null)
                return result;

            foreach (var categoryProp in data.Properties())
            {
                var categoryId = categoryProp.Name;
                var categoryFields = categoryProp.Value as JObject;

                // Get category label from options (fallback to categoryId if missing)
                var categoryLabel = options[categoryId]?["label"]?.ToString()
                                    ?? schema[categoryId]?["title"]?.ToString()
                                    ?? categoryId;

                foreach (var fieldProp in categoryFields.Properties())
                {
                    var fieldKey = fieldProp.Name;
                    if (fieldKey.EndsWith("_1")) // Skip metadata_token container
                        continue;

                    var fieldValue = fieldProp.Value?.ToString();

                    // Get field label from options
                    var fieldLabel = options[categoryId]?["fields"]?[fieldKey]?["label"]?.ToString()
                                     ?? schema[categoryId]?["properties"]?[fieldKey]?["title"]?.ToString()
                                     ?? fieldKey;

                    var composedKey = $"{categoryLabel}_{fieldLabel}";
                    result[composedKey] = fieldValue;
                }
            }

            return result;
        }
        public static async Task<Node> GetNode(long id)
        {
            RestClient client = new RestClient(Properties.Settings.Default.APIUrl + "v1/nodes/" + id.ToString());
            RestRequest request = new RestRequest("", Method.Get);

            try
            {
                string otcsTicket = await AuthAPI.AuthenticateUser();
                if (String.IsNullOrEmpty(otcsTicket))
                {
                    throw new Exception("Invalid otcsTicket");
                }

                request.AddHeader("OTCSTicket", otcsTicket);

                RestResponse response = await client.ExecuteAsync(request);
                response.ThrowIfError();

                if (!response.IsSuccessful)
                {
                    throw new Exception("v1/nodes is not succesful");
                }

                var content = JObject.Parse(response.Content);

                Node nodeInfo = new Node();

                if (content.TryGetValue("data", out var data))
                {
                    dynamic nodeData = data;

                    nodeInfo.ID = long.Parse(nodeData.id.ToString());
                    nodeInfo.ParentID = long.Parse(nodeData.parent_id.ToString());
                    nodeInfo.Name = nodeData.name.ToString();
                    nodeInfo.CreationDate = DateTime.Parse(nodeData.create_date.ToString());
                    nodeInfo.ModifiedDate = DateTime.Parse(nodeData.modify_date.ToString());
                    nodeInfo.SubType = Int32.Parse(nodeData.type.ToString());
                    nodeInfo.SubTypeName = nodeData.type_name.ToString();
                }

                return nodeInfo;
            }
            catch (Exception ex)
            {
                Trace.WriteLine("GetNode failed");
                Trace.WriteLine(ex.StackTrace);
                throw;
            }
            finally
            {
                client.Dispose();
            }
        }

        public static async Task<List<Permissions>> GetNodeRights(long id)
        {
            RestClient client = new RestClient(Properties.Settings.Default.APIUrl + "v2/nodes/" + id.ToString() + "/permissions");
            RestRequest request = new RestRequest("", Method.Get);
            
            try
            {
                string otcsTicket = await AuthAPI.AuthenticateUser();
                if (String.IsNullOrEmpty(otcsTicket))
                {
                    throw new Exception("Invalid otcsTicket");
                }

                request.AddHeader("OTCSTicket", otcsTicket);

                RestResponse response = await client.ExecuteAsync(request);
                response.ThrowIfError();

                if (!response.IsSuccessful)
                {
                    throw new Exception("/v2/nodes/{id}/permissions is not successful");
                }

                var content = JObject.Parse(response.Content);
                List<Permissions> permissionsList = new List<Permissions>();

                if (content.TryGetValue("results", out var results))
                {
                    //dynamic resultRes = results;
                    var data = results.ToList();

                    for (int i = 0; i < data.Count; i++)
                    {
                        dynamic permissionInfo = ((dynamic)(data[i])).data.permissions;

                        Permissions perm = new Permissions();

                        var rightId = permissionInfo.right_id.ToString();
                        if (!String.IsNullOrEmpty(rightId))
                        {
                            perm.RightID = long.Parse(rightId);
                        }

                        perm.RightType = permissionInfo.type.ToString();

                        var listOfPermissions = permissionInfo.permissions;
                        for (int a = 0; a < listOfPermissions.Count; a++)
                        {
                            perm.Rights.Add(listOfPermissions[a].ToString());
                        }

                        permissionsList.Add(perm);
                    }
                }

                return permissionsList;
            } catch (Exception ex)
            {
                Trace.WriteLine("GetNodeRights failed");
                Trace.WriteLine(ex.StackTrace);
                throw;
            } finally
            {
                client.Dispose();
            }
        }

        private static async Task<List<Node>> _GetChildNodesByPage(long id, int page)
        {
            RestClient client = new RestClient(Properties.Settings.Default.APIUrl + "v1/nodes/" + id.ToString() + "/nodes");
            RestRequest request = new RestRequest("", Method.Get);

            request.AddParameter("limit", 100);
            request.AddParameter("fields", "data");
            request.AddParameter("extra", false);
            request.AddParameter("page", page);

            try
            {
                string otcsTicket = await AuthAPI.AuthenticateUser();
                if (String.IsNullOrEmpty(otcsTicket))
                {
                    throw new Exception("Invalid otcsTicket");
                }

                request.AddHeader("OTCSTicket", otcsTicket);

                RestResponse response = await client.ExecuteAsync(request);
                //Console.WriteLine(response.Content.ToString());
                response.ThrowIfError();

                if (!response.IsSuccessful)
                {
                    throw new Exception("v1/nodes/{id}/nodes?page is not succesful");
                }

                var content = JObject.Parse(response.Content);
                List<Node> nodes = new List<Node>();

                if (content.TryGetValue("data", out var data))
                {
                    var dataList = data.ToList();

                    for (int i = 0; i < dataList.Count; i++)
                    {
                        dynamic node = dataList[i];

                        nodes.Add(new Node()
                        {
                            Name = node.name.ToString(),
                            ID = long.Parse(node.id.ToString()),
                            ParentID = long.Parse(node.parent_id.ToString()),
                            CreationDate = DateTime.Parse(node.create_date.ToString()),
                            ModifiedDate = DateTime.Parse(node.modify_date.ToString()),
                            SubType = Int32.Parse(node.type.ToString()),
                            SubTypeName = node.type_name.ToString(),
                        });
                    }
                }

                return nodes;
            } catch (Exception ex)
            {
                Trace.WriteLine("_GetChildNodesByPage failed");
                Trace.WriteLine(ex.StackTrace);
                throw;
            } finally
            {
                client.Dispose();
            }
        }

        public static async Task<List<Node>> GetChildNodes(long id)
        {
            RestClient client = new RestClient(Properties.Settings.Default.APIUrl + "v1/nodes/" + id.ToString() + "/nodes");
            RestRequest request = new RestRequest("", Method.Get);

            request.AddParameter("limit", 100); // max limit is 100, create new method to get by pages
            request.AddParameter("fields", "page_total");

            try
            {
                string otcsTicket = await AuthAPI.AuthenticateUser();
                if (String.IsNullOrEmpty(otcsTicket))
                {
                    throw new Exception("Invalid otcsTicket");
                }

                request.AddHeader("OTCSTicket", otcsTicket);

                RestResponse response = await client.ExecuteAsync(request);
                response.ThrowIfError();

                if (!response.IsSuccessful)
                {
                    throw new Exception("v1/nodes/{id}/nodes is not succesful");
                }

                var content = JObject.Parse(response.Content);
                List<Node> listOfNodes = new List<Node>(); 

                if (content.TryGetValue("page_total", out var pageTotal))
                {
                    int totalPages = Int32.Parse(pageTotal.ToString());

                    for (int i = 0; i < totalPages; i++)
                    {
                        var nodesInPage = await _GetChildNodesByPage(id, i + 1);
                        if (nodesInPage != null) {
                            listOfNodes.AddRange(nodesInPage);
                        }
                    }
                }

                //Console.WriteLine(content.ToString());

                return listOfNodes;
            } catch(Exception ex)
            {
                Trace.WriteLine("GetChildNodes failed");
                Trace.WriteLine(ex.StackTrace);
                throw;
            } finally
            {
                client.Dispose();
            }
        }

        public static string GetPath(long id)
        {
            RestClient client = new RestClient(Properties.Settings.Default.APIUrl + "v1/nodes/" + id.ToString() + "/ancestors");
            RestRequest request = new RestRequest("", Method.Get);

            try
            {
                string otdsTicket = AuthAPI.AuthenticateUserViaOTDS();
                if (String.IsNullOrEmpty(otdsTicket))
                {
                    throw new Exception("Invalid otdsTicket");
                }

                request.AddHeader("OTDSTicket", otdsTicket);

                RestResponse response = client.Execute(request);
                response.ThrowIfError();

                if (!response.IsSuccessful)
                {
                    throw new Exception("v1/nodes/{id}/ancestors is not succesful");
                }

                var content = JObject.Parse(response.Content);
                string output = "";

                if (content.TryGetValue("ancestors", out var data))
                {
                    var ancestorData = data.ToList();
                    

                    for (int i = 0; i < ancestorData.Count; i++)
                    {
                        dynamic ancestor = ancestorData[i];

                        if (i + 1 < ancestorData.Count)
                        {
                            output += ancestor.name.ToString() + "/";
                        } else
                        {
                            output += ancestor.name.ToString();
                        }
                        
                    }
                }

                return output;
            }
            catch (Exception ex)
            {
                Trace.WriteLine("GetPath failed");
                Trace.WriteLine(ex.StackTrace);
                throw;
            }
            finally
            {
                client.Dispose();
            }
        }
    }
}
