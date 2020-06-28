using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace SqlToJson
{
    public static class Query
    {
        [FunctionName(nameof(Query))]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = "Query/{queryName}")] HttpRequest req,
            string queryName,
            ILogger log)
        {
            return new FileStreamResult(
                await QueryStream.OpenStream( 
                    connectionString: Environment.GetEnvironmentVariable("SQLTOJSON_CONNECTION"),
                    query: Environment.GetEnvironmentVariable(string.Concat("SQLTOJSON_QUERY_", queryName))
                ),
                "application /json"
            );
        }
    }
}
