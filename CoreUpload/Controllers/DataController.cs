using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.DataLake.Store;
using Microsoft.Rest;
using Microsoft.Rest.Azure.Authentication;

using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CoreUpload.Controllers
{
    [Route("api/[controller]")]
    public class DataController : Controller
    {
        private static ServiceClientCredentials GetCredentials(string tenant, Uri tokenAudience, string clientId, string secretKey)
        {
            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());

            var serviceSettings = ActiveDirectoryServiceSettings.Azure;
            serviceSettings.TokenAudience = tokenAudience;

            var creds = ApplicationTokenProvider.LoginSilentAsync(
             tenant,
             clientId,
             secretKey,
             serviceSettings).GetAwaiter().GetResult();
            return creds;
        }

        private static AdlsClient GetAdlsClient()
        {
            const string tenant = "bb6c5fda-8620-4d74-ba84-e11a629258c0";
            const string applicationId = "49906591-0360-496b-8f77-b31c544df32e";
            const string secretKey = "WHCy3T7RzOg/2jllvWn8uY2aCMKKMZok2KAoDHUiAXY=";

            Uri adlTokenAudience = new Uri(@"https://datalake.azure.net/");
            ServiceClientCredentials credentials = GetCredentials(tenant, adlTokenAudience, applicationId, secretKey);
            AdlsClient client = AdlsClient.CreateClient("roer.azuredatalakestore.net", credentials);
            return client;
        }

        private AdlsClient Client;

        public DataController()
        {
            Client = GetAdlsClient();
        }

        private async Task Upload(StreamReader reader)
        {
            string fileName = $"/files/{Guid.NewGuid().ToString()}";
            using (AdlsOutputStream file = await Client.CreateFileAsync(fileName, IfExists.Overwrite))
            {
                int bufferSize = 1024 * 1024;
                char[] buffer = new char[bufferSize];
                using (var writer = new StreamWriter(file))
                {
                    int totalRead = 0;
                    Console.WriteLine("Reading...");
                    while (!reader.EndOfStream)
                    {
                        int read = await reader.ReadAsync(buffer, 0, bufferSize);
                        totalRead += read;
                        Console.WriteLine($"Read: {totalRead}");
                        await writer.WriteAsync(buffer, 0, read);
                    }
                    Console.WriteLine("...done reading!");
                }
            }
        }

        [HttpPost]
        public async Task<IActionResult> Post()
        {
            var request = HttpContext.Request;
            var body = request.Body;
            
            using (var reader = new StreamReader(request.Body, Encoding.UTF8))
            {
                await Upload(reader);
            }

            return NoContent();
        }
    }
}
