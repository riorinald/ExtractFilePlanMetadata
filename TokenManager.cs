using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DownloadFilePlan
{
    public class TokenManager
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();
        private static readonly TimeSpan TokenLifetime = TimeSpan.FromMinutes(3);
        private static string _currentToken;
        private static DateTime _tokenExpiryTime;
        private static readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

        public static async Task<string> GetValidTokenAsync()
        {
            await _semaphore.WaitAsync();
            try
            {
                if (string.IsNullOrEmpty(_currentToken) || DateTime.Now.AddSeconds(30) >= _tokenExpiryTime)
                {
                    // Refresh token
                    string userName, password;
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
                    _currentToken = AuthAPI.AuthenticateUserViaOTDS();
                    _tokenExpiryTime = DateTime.Now.Add(TokenLifetime);
                    Logger.Info("Token refreshed successfully");
                }
                return _currentToken;
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }
}
