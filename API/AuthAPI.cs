using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RestSharp;

namespace DownloadFilePlan
{
    class AuthAPI
    {
        private static DateTime lastRefresh = DateTime.Now;
        private static string otcsTicket = "";
        private static Tuple<string, string> authInfo = new Tuple<string, string>("", "");

        private static DateTime lastRefresh_OTDS = DateTime.Now;
        private static string otdsTicket = "";

        public static void SetAuthInfo(string user, string pass)
        {
            authInfo = new Tuple<string, string>(user, pass);
        }

        public static string GetTicket()
        {
            return otcsTicket;
        }

        public static string GetOTDSTicket()
        {
            return otdsTicket;
        }

        public static string AuthenticateUserViaOTDS()
        {
            // Don't get ticket again if not it still is within refresh time
            if (DateTime.Compare(lastRefresh_OTDS.AddMinutes(Properties.Settings.Default.RefreshAuthTokenEveryXMinutes), DateTime.Now)  > 0 && !String.IsNullOrEmpty(otdsTicket))
            {
                return otdsTicket;
            }

            RestClient client = new RestClient(Properties.Settings.Default.OTDSApiURL + "authentication/credentials");
            RestRequest request = new RestRequest("", Method.Post);
            request.AddHeader("Content-Type", "application/json");
            object requestObject = new
            {
                userName = authInfo.Item1,
                password = authInfo.Item2,
            };

            request.AddJsonBody(requestObject);

            try
            {
                Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Authenticating user " + authInfo.Item1);

                RestResponse response = client.Execute(request);
                response.ThrowIfError();

                if (response.Content != null)
                {
                    var content = JObject.Parse(response.Content);

                    if (content.TryGetValue("ticket", out var ticket))
                    {
                        otdsTicket = ticket.ToString();
                        lastRefresh_OTDS = DateTime.Now;
                        Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Authenticated: " + authInfo.Item1);
                    }
                }
            } catch(Exception ex)
            {
                Console.WriteLine("OTDS Auth failed");
                Trace.WriteLine("OTDS Authentication failed");
                Trace.WriteLine(ex.StackTrace);
                return otdsTicket;
            } finally
            {
                client.Dispose();
            }

            return otdsTicket;
        }

        public static async Task<string> AuthenticateUser()
        {
            // Don't get ticket again if not it still is within refresh time
            if (DateTime.Compare(lastRefresh.AddMinutes(Properties.Settings.Default.RefreshAuthTokenEveryXMinutes), DateTime.Now) > 0 && !String.IsNullOrEmpty(otcsTicket))
            {
                return otcsTicket;
            }

            RestClient client = new RestClient(Properties.Settings.Default.APIUrl + "v1/auth");
            RestRequest request = new RestRequest("", Method.Post);
            request.AddHeader("Content-Type", "application/x-www-form-urlencoded");
            request.AddParameter("username", authInfo.Item1);
            request.AddParameter("password", authInfo.Item2);

            try
            {
                Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Authenticating user " + authInfo.Item1);

                RestResponse response = await client.ExecuteAsync(request);
                response.ThrowIfError();

                var content = JObject.Parse(response.Content);

                if (content.TryGetValue("ticket", out var ticket))
                {
                    otcsTicket = ticket.ToString();
                    lastRefresh = DateTime.Now;

                }

                Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|Success!");
            }
            catch (Exception e)
            {
                Trace.WriteLine(DateTime.Now.ToString("HH:mm:ss") + "|OTDS Authentication failed. Message: " + e.Message);
                return otcsTicket;
            }
            finally
            {
                client.Dispose();
            }
            return otcsTicket;
        }
    }
}
