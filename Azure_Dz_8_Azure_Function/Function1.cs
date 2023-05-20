using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Data.Tables;
using System.Linq;

namespace Azure_Dz_8_Azure_Function
{
    public class Function1
    {
        private readonly ILogger _logger;
        public Function1(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<Function1>();
        }

        [FunctionName("Set")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            [Table("ShortUrls")] TableClient tableClient,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string href = req.Query["href"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            href = href ?? data?.href;

            if (string.IsNullOrEmpty(href))
            {
                return new OkObjectResult(
                "Please enter the parameter href aka " +
                "http://localhost:7071/api/Set?href=https://anotherSite.com");
            }
            UrlKey urlKey;
            var result = await tableClient.GetEntityIfExistsAsync<UrlKey>("1", "Key");
            if (result.HasValue == false)
            {
                urlKey = new UrlKey
                {
                    Id = 1024,
                    PartitionKey = "1",
                    RowKey = "Key",
                };
                await tableClient.UpsertEntityAsync(urlKey);
            }
            else
            {
                urlKey = result.Value;
            }
            int index = urlKey.Id;
            const string ALPHABET = "ABCDEFGHIGKLMNOPQRSTUVWXYZ";
            string code = string.Empty;
            while (index > 0)
            {
                code += ALPHABET[index % ALPHABET.Length];
                index /= ALPHABET.Length;
            }
            code = string.Join(string.Empty, code.Reverse());
            urlKey.Id++;
            await tableClient.UpsertEntityAsync<UrlKey>(urlKey);
            UrlData urlData = new UrlData
            {
                Id = code,
                Url = href,
                Count = 1,
                PartitionKey = $"{code[0]}",
                RowKey = code,
            };
            await tableClient.UpsertEntityAsync<UrlData>(urlData);
            return new OkObjectResult(
            $"Original url: {urlData.Url} " +
            $"\nShort url: {urlData.RowKey}");
        
        }

        [FunctionName("Go")]
        public async Task<IActionResult> Go(
            [HttpTrigger(
            AuthorizationLevel.Anonymous, "get", "post", Route = "Go/{shortUrl}")] HttpRequest req, string shortUrl,
            [Table("ShortUrls")] TableClient tableClient,
            [Queue("counts")] IAsyncCollector<string> queue,
            ILogger logger)
        {
            if (string.IsNullOrWhiteSpace(shortUrl))
            {
                return new BadRequestResult();
            }
            shortUrl = shortUrl.ToUpper();
            // TableClient tableClient = await GetTableClient();
            var result = await tableClient
            .GetEntityIfExistsAsync<UrlData>(shortUrl[0].ToString(), shortUrl);
            var url = "https://defaultUrl.xxx";
            if (result.HasValue && result.Value is UrlData data)
            {
                url = data.Url;
                await queue.AddAsync(data.RowKey);
            }
            return new RedirectResult(url);
        }

        [FunctionName("ProcessQueue")]
        public async Task ProcessQueue(
            [QueueTrigger("counts")] string shortCode,
            [Table("ShortUrls")] TableClient tableClient,
            ILogger logger)
        {
            // TableClient tableClient = await GetTableClient();
            var result = await tableClient
            .GetEntityIfExistsAsync<UrlData>(shortCode[0].ToString(), shortCode); if (result.HasValue && result.Value is UrlData data)
            {
                data.Count++;
                await tableClient.UpsertEntityAsync<UrlData>(data);
            }
        }
    }
}
