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
    class MembersAPI
    {
        public static async Task<Member> GetMemberById(long id)
        {
            RestClient client = new RestClient(Properties.Settings.Default.APIUrl + "v1/members/" + id.ToString());
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
                    throw new Exception("v1/members is not succesful");
                }

                var content = JObject.Parse(response.Content);

                Member memberInfo = new Member();

                if (content.TryGetValue("data", out var data))
                {
                    dynamic memberData = data;

                    memberInfo.ID = long.Parse(memberData.id.ToString());
                    memberInfo.Name = memberData.name.ToString();
                    memberInfo.FirstName = memberData.first_name.ToString();
                    memberInfo.LastName = memberData.last_name.ToString();
                }

                return memberInfo;
            } catch (Exception ex)
            {
                Trace.WriteLine("GetMemberById failed");
                Trace.WriteLine(ex.StackTrace);
                throw;
            } finally
            {
                client.Dispose();
            }
        }
    }
}
