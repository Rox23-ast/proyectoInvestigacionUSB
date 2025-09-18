using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Data.SqlClient;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using MySql.Data.MySqlClient;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Text.Json;
using ProyectoBackendCsharp.Services;
using NpgsqlTypes;

[Route("api/{projectName}/functions")]
[ApiController]
public class FunctionController : ControllerBase
{
    private readonly ControlConexion controlConexion;
    private readonly IConfiguration _configuration;

    public FunctionController(ControlConexion controlConexion, IConfiguration configuration)
    {
        this.controlConexion = controlConexion ?? throw new ArgumentNullException(nameof(controlConexion));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }

    [HttpPost("execute")]
    public IActionResult ExecuteFunction(string projectName, [FromBody] Dictionary<string, JsonElement> functionRequest)
    {
        if (functionRequest == null || !functionRequest.ContainsKey("function") || string.IsNullOrWhiteSpace(functionRequest["function"].GetString()))
            return BadRequest("The function cannot be empty.");

        try
        {
            string functionName = functionRequest["function"].GetString()!;
            string provider = _configuration["DatabaseProvider"] ?? throw new InvalidOperationException("DatabaseProvider not configured.");
            bool shouldEncrypt = functionRequest.ContainsKey("shouldEncrypt") && functionRequest["shouldEncrypt"].GetBoolean();

            (DataTable resultado, Dictionary<string, object?> outputParams) result;
            switch (provider)
            {
                case "SqlServer":
                    result = ExecutePostgresFunction(functionName, functionRequest, shouldEncrypt);
                    break;
                case "Postgres":
                    result = ExecutePostgresFunction(functionName, functionRequest, shouldEncrypt);
                    break;
                case "Oracle":
                    result = ExecutePostgresFunction(functionName, functionRequest, shouldEncrypt);
                    break;
                case "MySql":
                    result = ExecutePostgresFunction(functionName, functionRequest, shouldEncrypt);
                    break;
                case "LocalDb":
                    result = ExecutePostgresFunction(functionName, functionRequest, shouldEncrypt);
                    break;
                default:
                    throw new InvalidOperationException("Unsupported database provider.");
            }

            var response = ConvertDataTableToResponse(result.Item1);
            return Ok(new { result = response, outputParams = result.Item2 });
        }
        catch (Exception ex)
        {
            return StatusCode(500, $"Internal server error: {ex.Message}. Function: {functionRequest["function"]}");
        }
    }


private (DataTable, Dictionary<string, object?>) ExecutePostgresFunction(string functionName, Dictionary<string, JsonElement> functionRequest, bool shouldEncrypt = false)
{
    controlConexion.AbrirBd();
    try
    {
        var outputParams = new Dictionary<string, object?>();
        var connection = controlConexion.GetConnection() as NpgsqlConnection;
        if (connection == null)
        {
            throw new InvalidOperationException("Invalid connection type for Postgres.");
        }
        using (var command = new NpgsqlCommand($"SELECT {functionName}(", connection))
        {
            if (functionRequest.ContainsKey("parameters"))
            {
                var parameters = functionRequest["parameters"].EnumerateObject().ToList();
                for (int i = 0; i < parameters.Count; i++)
                {
                    var param = parameters[i];
                    string paramName = $"@{param.Name}";
                    object? paramValue = param.Value.ValueKind switch
                    {
                        JsonValueKind.Number => param.Value.GetDouble(),
                        JsonValueKind.String => param.Value.GetString(),
                        JsonValueKind.True => true,
                        JsonValueKind.False => false,
                        JsonValueKind.Null => DBNull.Value,
                        JsonValueKind.Object => param.Value.GetRawText(),
                        JsonValueKind.Array => param.Value.GetRawText(),
                        _ => param.Value.GetRawText()
                    };

                    // Para input_json y input_json_list, asegúrate de que se envíen como texto
                    if (param.Name == "input_json" || param.Name == "input_json_list")
                    {
                        paramValue = param.Value.GetRawText();
                    }

                    if (paramValue is string stringValue)
                    {
                        if (!string.IsNullOrEmpty(stringValue))
                        {
                            stringValue = stringValue.Trim();
                            if (IsValidJson(stringValue))
                            {
                                var jsonDoc = JsonDocument.Parse(stringValue);
                                var jsonObject = jsonDoc.RootElement.Clone();
                                EncriptarPasswordEnJson(ref jsonObject, shouldEncrypt);
                                paramValue = jsonObject.GetRawText();
                            }
                            else if (IsPasswordField(param.Name) && shouldEncrypt)
                            {
                                paramValue = BCrypt.Net.BCrypt.HashPassword(stringValue);
                            }
                        }
                    }

                    command.Parameters.AddWithValue(paramName, paramValue ?? DBNull.Value);
                    command.CommandText += $"{paramName}{(i < parameters.Count - 1 ? "," : "")}";
                }
            }
            command.CommandText += ")";
            var result = new DataTable();
            string jsonResult;
            using (var reader = command.ExecuteReader())
            {
                if (reader.Read())
                {
                    jsonResult = reader[0]?.ToString() ?? "{}";
                }
                else
                {
                    jsonResult = "{}";
                }
            }
            outputParams["ReturnValue"] = jsonResult;
            // Convertir el resultado JSON a DataTable
            if (!string.IsNullOrEmpty(jsonResult))
            {
                result = JsonToDataTable(jsonResult);
            }
            return (result, outputParams);
        }
    }
    finally
    {
        controlConexion.CerrarBd();
    }
}
    private DataTable JsonToDataTable(string jsonString)
    {
        var dt = new DataTable();
        using (var jsonDoc = JsonDocument.Parse(jsonString))
        {
            var root = jsonDoc.RootElement;
            if (root.ValueKind == JsonValueKind.Array)
            {
                var firstElement = root.EnumerateArray().FirstOrDefault();
                if (firstElement.ValueKind == JsonValueKind.Object)
                {
                    foreach (var property in firstElement.EnumerateObject())
                    {
                        dt.Columns.Add(property.Name);
                    }

                    foreach (var element in root.EnumerateArray())
                    {
                        var row = dt.NewRow();
                        foreach (var property in element.EnumerateObject())
                        {
                            row[property.Name] = property.Value.ToString();
                        }
                        dt.Rows.Add(row);
                    }
                }
            }
        }
        return dt;
    }

    private List<Dictionary<string, object?>> ConvertDataTableToResponse(DataTable dataTable)
    {
        var response = new List<Dictionary<string, object?>>();
        foreach (DataRow fila in dataTable.Rows)
        {
            var propiedades = new Dictionary<string, object?>();
            foreach (DataColumn columna in dataTable.Columns)
            {
                propiedades[columna.ColumnName] = fila[columna] == DBNull.Value ? null : fila[columna];
            }
            response.Add(propiedades);
        }
        return response;
    }

    private object? ConvertJsonElement(JsonElement jsonElement)
    {
        return jsonElement.ValueKind switch
        {
            JsonValueKind.String => jsonElement.GetString(),
            JsonValueKind.Number => jsonElement.TryGetInt64(out var longValue) ? (object)longValue : jsonElement.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => DBNull.Value,
            JsonValueKind.Object => jsonElement.GetRawText(),
            JsonValueKind.Array => jsonElement.GetRawText(),
            _ => throw new InvalidOperationException($"Unsupported JsonValueKind: {jsonElement.ValueKind}"),
        };
    }

    private void EncriptarPasswordEnJson(ref JsonElement jsonObject, bool shouldEncrypt = false)
    {
        if (!shouldEncrypt) return;

        if (jsonObject.ValueKind == JsonValueKind.Object)
        {
            var dict = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(jsonObject.GetRawText());

            if (dict != null)
            {
                bool changed = false;
                foreach (var key in dict.Keys.ToList())
                {
                    if (IsPasswordField(key) && dict[key].ValueKind == JsonValueKind.String)
                    {
                        var plainPassword = dict[key].GetString();
                        if (!string.IsNullOrEmpty(plainPassword))
                        {
                            dict[key] = JsonDocument.Parse($"\"{BCrypt.Net.BCrypt.HashPassword(plainPassword)}\"").RootElement;
                            changed = true;
                        }
                    }
                    else if (dict[key].ValueKind == JsonValueKind.Object)
                    {
                        var subElement = dict[key];
                        EncriptarPasswordEnJson(ref subElement, shouldEncrypt);
                        dict[key] = subElement;
                    }
                }

                if (changed)
                {
                    jsonObject = JsonDocument.Parse(JsonSerializer.Serialize(dict)).RootElement;
                }
            }
        }
    }

    private bool IsPasswordField(string fieldName)
    {
        var passwordKeys = new[] { "password", "contrasena", "passw" };
        return passwordKeys.Any(pk => fieldName.IndexOf(pk, StringComparison.OrdinalIgnoreCase) >= 0);
    }

    private bool IsValidJson(string strInput)
    {
        if (string.IsNullOrWhiteSpace(strInput)) { return false; }
        strInput = strInput.Trim();
        if ((strInput.StartsWith("{") && strInput.EndsWith("}")) || (strInput.StartsWith("[") && strInput.EndsWith("]")))
        {
            try
            {
                var obj = System.Text.Json.JsonSerializer.Deserialize<object>(strInput);
                return true;
            }
            catch
            {
                return false;
            }
        }
        else
        {
            return false;
        }
    }


}
