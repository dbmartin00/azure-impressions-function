using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Primitives;

public class HttpTriggerImpressions
{
    private readonly ILogger<HttpTriggerImpressions> _logger;
    private readonly string? _connectionString;

    public HttpTriggerImpressions(ILogger<HttpTriggerImpressions> logger)
    {
        _logger = logger;
        _connectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
    }

    [Function("ImpressionsWebhook")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequestData req)
    {
        try
        {
            // Log all query parameters properly
            _logger.LogInformation("Query Parameters Received:");

            foreach (KeyValuePair<string, StringValues> param in req.Query)
            {
                _logger.LogInformation($"{param.Key}: {param.Value.ToString()}");
            }

            // Decode the request body (handles Base64, Gzip, and plain JSON)
            string jsonBody = await DecodeRequestBody(req.Body);
            // _logger.LogInformation($"Decoded JSON body: {jsonBody}");

            // Deserialize JSON
            List<Impression>? impressions = JsonSerializer.Deserialize<List<Impression>>(jsonBody) ?? new List<Impression>();

            if (impressions.Count == 0)
            {
                _logger.LogWarning("No impressions received.");
                return req.CreateResponse(System.Net.HttpStatusCode.BadRequest);
            }

            // Ensure table exists
            _logger.LogInformation("Ensuring table exists...");
            await EnsureTableExists();

            // Insert impressions
            _logger.LogInformation("Inserting " + impressions.Count + " impressions...");
            await InsertImpressions(impressions);

            // Return success response
            var response = req.CreateResponse(System.Net.HttpStatusCode.OK);
            await response.WriteStringAsync("Impressions processed successfully.");
            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error processing impressions: {ex.Message}");
            var response = req.CreateResponse(System.Net.HttpStatusCode.InternalServerError);
            await response.WriteStringAsync("Error processing impressions.");
            return response;
        }
    }

    private async Task<string> DecodeRequestBody(Stream requestBodyStream)
    {
        // Copy the request stream into a MemoryStream to allow seeking
        using var memoryStream = new MemoryStream();
        await requestBodyStream.CopyToAsync(memoryStream);
        memoryStream.Seek(0, SeekOrigin.Begin); // Reset to beginning

        // Check if the request body is Gzipped
        if (IsGzipped(memoryStream))
        {
            _logger.LogInformation("Detected Gzip compression.");
            return await DecompressGzip(memoryStream);
        }

        // If not Gzipped, read as normal text
        memoryStream.Seek(0, SeekOrigin.Begin); // Reset position
        using var reader = new StreamReader(memoryStream, Encoding.UTF8);
        return await reader.ReadToEndAsync();
    }


    private bool IsGzipped(Stream stream)
    {
        byte[] header = new byte[2];
        stream.Seek(0, SeekOrigin.Begin); // Ensure we're at the beginning
        stream.Read(header, 0, 2);
        stream.Seek(0, SeekOrigin.Begin); // Reset to allow re-reading

        return header[0] == 0x1F && header[1] == 0x8B; // Gzip magic number
    }



    private async Task<string> DecompressGzip(Stream compressedStream)
    {
        using var decompressedStream = new MemoryStream();
        using var gzipStream = new GZipStream(compressedStream, CompressionMode.Decompress);
        await gzipStream.CopyToAsync(decompressedStream);
        decompressedStream.Seek(0, SeekOrigin.Begin);

        using var reader = new StreamReader(decompressedStream, Encoding.UTF8);
        return await reader.ReadToEndAsync();
    }


    private async Task EnsureTableExists()
    {
        var query = @"
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Impressions' AND xtype='U')
        CREATE TABLE Impressions (
            Id INT IDENTITY(1,1) PRIMARY KEY,
            [Key] VARCHAR(255),  -- Use square brackets for reserved keywords
            Split VARCHAR(255),
            EnvironmentId VARCHAR(255),
            EnvironmentName VARCHAR(255),
            Treatment VARCHAR(255),
            Time BIGINT,
            Label VARCHAR(255),
            SplitVersionNumber BIGINT,
            Sdk VARCHAR(255),
            SdkVersion VARCHAR(255),
            CreatedAt DATETIME DEFAULT GETDATE()
        )";
        
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        using var command = new SqlCommand(query, connection);
        await command.ExecuteNonQueryAsync();
    }


    private async Task InsertImpressions(List<Impression> impressions)
    {
        var query = @"
        INSERT INTO Impressions 
        ([Key], Split, EnvironmentId, EnvironmentName, Treatment, Time, Label, SplitVersionNumber, Sdk, SdkVersion) 
        VALUES (@key, @split, @environmentId, @environmentName, @treatment, @time, @label, @splitVersionNumber, @sdk, @sdkVersion)";

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        foreach (var impression in impressions)
        {
            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@key", impression.key);
            command.Parameters.AddWithValue("@split", impression.split);
            command.Parameters.AddWithValue("@environmentId", impression.environmentId);
            command.Parameters.AddWithValue("@environmentName", impression.environmentName);
            command.Parameters.AddWithValue("@treatment", impression.treatment);
            command.Parameters.AddWithValue("@time", impression.time);
            command.Parameters.AddWithValue("@label", impression.label);
            command.Parameters.AddWithValue("@splitVersionNumber", impression.splitVersionNumber);
            command.Parameters.AddWithValue("@sdk", impression.sdk);
            command.Parameters.AddWithValue("@sdkVersion", impression.sdkVersion);
            await command.ExecuteNonQueryAsync();
        }
    }


    public partial class Impression
    {
        public required string key { get; set; }
        public required string split { get; set; }
        public required string environmentId { get; set; }
        public required string environmentName { get; set; }
        public required string treatment { get; set; }
        public required long time { get; set; }
        public required string label { get; set; }
        public required long splitVersionNumber { get; set; }
        public required string sdk { get; set; }
        public required string sdkVersion { get; set; }
    }
    
}
