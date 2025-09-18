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

[Route("api/{projectName}/procedures")]
[ApiController]
public class ProcedureController : ControllerBase
{
    private readonly ControlConexion controlConexion;
    private readonly IConfiguration _configuration;

    public ProcedureController(ControlConexion controlConexion, IConfiguration configuration)
    {
        this.controlConexion = controlConexion ?? throw new ArgumentNullException(nameof(controlConexion));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }

    [HttpPost("execute")]
    public IActionResult ExecuteProcedure(string projectName, [FromBody] Dictionary<string, JsonElement> procedureRequest)
    {
        if (procedureRequest == null || !procedureRequest.ContainsKey("procedure") || string.IsNullOrWhiteSpace(procedureRequest["procedure"].GetString()))
            return BadRequest("The procedure cannot be empty.");

        try
        {
            string procedureName = procedureRequest["procedure"].GetString()!;
            string provider = _configuration["DatabaseProvider"] ?? throw new InvalidOperationException("DatabaseProvider not configured.");
            bool shouldEncrypt = procedureRequest.ContainsKey("shouldEncrypt") && procedureRequest["shouldEncrypt"].GetBoolean();

            (DataTable resultado, Dictionary<string, object?> outputParams) result;
            switch (provider)
            {
                case "SqlServer":
                    result = ExecutePostgresProcedure(procedureName, procedureRequest, shouldEncrypt);
                    break;
                case "Postgres":
                    result = ExecutePostgresProcedure(procedureName, procedureRequest, shouldEncrypt);
                    break;
                case "Oracle":
                    result = ExecutePostgresProcedure(procedureName, procedureRequest, shouldEncrypt);
                    break;
                case "MySql":
                    result = ExecutePostgresProcedure(procedureName, procedureRequest, shouldEncrypt);
                    break;
                case "LocalDb":
                    result = ExecutePostgresProcedure(procedureName, procedureRequest, shouldEncrypt);
                    break;
                default:
                    throw new InvalidOperationException("Unsupported database provider.");
            }

            var response = ConvertDataTableToResponse(result.Item1);
            return Ok(new { result = response, outputParams = result.Item2 });
        }
        catch (Exception ex)
        {
            return StatusCode(500, $"Internal server error: {ex.Message}. Procedure: {procedureRequest["procedure"]}");
        }
    }

private (DataTable, Dictionary<string, object?>) ExecutePostgresProcedure(string procedureName, Dictionary<string, JsonElement> procedureRequest, bool shouldEncrypt = false)
{
    controlConexion.AbrirBd();
    try
    {
        var outputParams = new Dictionary<string, object?>();
        var result = new DataTable();

        var connection = controlConexion.GetConnection() as NpgsqlConnection;
        if (connection == null)
        {
            throw new InvalidOperationException("Invalid connection type for Postgres.");
        }

        using (var command = new NpgsqlCommand(procedureName, connection))
        {
            command.CommandType = CommandType.StoredProcedure;

            if (procedureName == "insert_json_entity" || procedureName == "update_json_entity" || 
                procedureName == "delete_json_entity" || procedureName == "select_json_entity")
            {
                if (!procedureRequest.ContainsKey("parameters"))
                    throw new ArgumentException("Missing parameters for JSON entity procedure.");

                var parameters = procedureRequest["parameters"];

                if (procedureName == "insert_json_entity" || procedureName == "update_json_entity")
                {
                    if (!parameters.TryGetProperty("table_name", out var tableNameElement) ||
                        !parameters.TryGetProperty("json_data", out var jsonDataElement))
                        throw new ArgumentException("Missing required parameters: table_name and json_data.");

                    string tableName = tableNameElement.GetString() ?? string.Empty;
                    var jsonData = jsonDataElement.Clone();

                    if (shouldEncrypt)
                    {
                        EncriptarPasswordEnJson(ref jsonData, true);
                    }

                    command.Parameters.AddWithValue("p_table_name", NpgsqlDbType.Text, tableName);
                    command.Parameters.AddWithValue("json_data", NpgsqlDbType.Jsonb, jsonData.GetRawText() ?? "{}");

                    if (procedureName == "update_json_entity" && parameters.TryGetProperty("where_condition", out var whereConditionElement))
                    {
                        command.Parameters.AddWithValue("where_condition", NpgsqlDbType.Text, whereConditionElement.GetString() ?? string.Empty);
                    }
                }
                else if (procedureName == "delete_json_entity")
                {
                    if (!parameters.TryGetProperty("table_name", out var tableNameElement) ||
                        !parameters.TryGetProperty("where_condition", out var whereConditionElement))
                        throw new ArgumentException("Missing required parameters: table_name and where_condition.");

                    command.Parameters.AddWithValue("p_table_name", NpgsqlDbType.Text, tableNameElement.GetString() ?? string.Empty);
                    command.Parameters.AddWithValue("where_condition", NpgsqlDbType.Text, whereConditionElement.GetString() ?? string.Empty);
                }
else if (procedureName == "select_json_entity")
{
    if (!parameters.TryGetProperty("table_name", out var tableNameElement))
        throw new ArgumentException("Missing required parameter: table_name.");

    command.Parameters.AddWithValue("p_table_name", NpgsqlDbType.Text, tableNameElement.GetString() ?? string.Empty);

    if (parameters.TryGetProperty("select_columns", out var selectColumnsElement))
                            command.Parameters.AddWithValue("select_columns", NpgsqlDbType.Text, selectColumnsElement.GetString() ?? "*");


    if (parameters.TryGetProperty("where_condition", out var whereConditionElement))
        command.Parameters.AddWithValue("where_condition", NpgsqlDbType.Text, whereConditionElement.GetString() ?? string.Empty);
    
    if (parameters.TryGetProperty("order_by", out var orderByElement))
        command.Parameters.AddWithValue("order_by", NpgsqlDbType.Text, orderByElement.GetString() ?? string.Empty);
    
    if (parameters.TryGetProperty("limit_clause", out var limitClauseElement))
        command.Parameters.AddWithValue("limit_clause", NpgsqlDbType.Text, limitClauseElement.GetString() ?? string.Empty);

    // Manejar json_data como json_params
    if (parameters.TryGetProperty("json_data", out var jsonDataElement))
        command.Parameters.AddWithValue("json_params", NpgsqlDbType.Jsonb, jsonDataElement.GetRawText() ?? "{}");
    else
        command.Parameters.AddWithValue("json_params", NpgsqlDbType.Jsonb, "{}");

    command.Parameters.Add(new NpgsqlParameter("result", NpgsqlDbType.Jsonb) { Direction = ParameterDirection.Output });
}

                var mensajeParam = new NpgsqlParameter("mensaje", NpgsqlDbType.Text)
                {
                    Direction = ParameterDirection.InputOutput,
                    Value = DBNull.Value
                };
                command.Parameters.Add(mensajeParam);
                outputParams["mensaje"] = null;
            }
            else
            {
                // Manejo de parámetros genéricos para otros procedimientos almacenados
                if (procedureRequest.ContainsKey("parameters"))
                {
                    var parameters = procedureRequest["parameters"].EnumerateObject();
                    foreach (var param in parameters)
                    {
                        string paramName = param.Name;
                        object? paramValue;
                        NpgsqlDbType dbType;

                        switch (param.Value.ValueKind)
                        {
                            case JsonValueKind.Object:
                            case JsonValueKind.Array:
                                var jsonElement = param.Value.Clone();
                                if (shouldEncrypt)
                                {
                                    EncriptarPasswordEnJson(ref jsonElement, shouldEncrypt);
                                }
                                paramValue = jsonElement.GetRawText() ?? "{}";
                                dbType = NpgsqlDbType.Jsonb;
                                break;
                            case JsonValueKind.String:
                                string stringValue = param.Value.GetString() ?? "";
                                if (IsPasswordField(paramName))
                                {
                                    paramValue = !string.IsNullOrEmpty(stringValue) ? BCrypt.Net.BCrypt.HashPassword(stringValue) : DBNull.Value;
                                }
                                else if (IsValidJson(stringValue))
                                {
                                    var jsonDoc = JsonDocument.Parse(stringValue);
                                    var jsonObject = jsonDoc.RootElement.Clone();
                                    if (shouldEncrypt)
                                    {
                                        EncriptarPasswordEnJson(ref jsonObject, shouldEncrypt);
                                    }
                                    paramValue = jsonObject.GetRawText() ?? "{}";
                                    dbType = NpgsqlDbType.Jsonb;
                                }
                                else
                                {
                                    paramValue = stringValue;
                                }
                                dbType = NpgsqlDbType.Text;
                                break;
                            case JsonValueKind.Number:
                                if (param.Value.TryGetInt32(out int intValue))
                                {
                                    paramValue = intValue;
                                    dbType = NpgsqlDbType.Integer;
                                }
                                else
                                {
                                    paramValue = param.Value.GetDouble();
                                    dbType = NpgsqlDbType.Double;
                                }
                                break;
                            case JsonValueKind.True:
                            case JsonValueKind.False:
                                paramValue = param.Value.GetBoolean();
                                dbType = NpgsqlDbType.Boolean;
                                break;
                            case JsonValueKind.Null:
                                paramValue = DBNull.Value;
                                dbType = NpgsqlDbType.Unknown;
                                break;
                            default:
                                throw new InvalidOperationException($"Unsupported JsonValueKind: {param.Value.ValueKind}");
                        }

                        command.Parameters.Add(new NpgsqlParameter(paramName, dbType) { Value = paramValue ?? DBNull.Value });
                    }
                }

                // Manejo de parámetros de salida
                if (procedureRequest.ContainsKey("outputParameters"))
                {
                    var outputParamsList = procedureRequest["outputParameters"].EnumerateArray();
                    foreach (var param in outputParamsList)
                    {
                        string paramName = param.GetString() ?? throw new InvalidOperationException("Output parameter name cannot be null.");
                        var outputParam = new NpgsqlParameter(paramName, NpgsqlDbType.Text)
                        {
                            Direction = ParameterDirection.Output
                        };
                        command.Parameters.Add(outputParam);
                        outputParams[paramName] = null;
                    }
                }

                if (procedureRequest.ContainsKey("errorOutputParameter"))
                {
                    string errorParamName = procedureRequest["errorOutputParameter"].GetString() ?? throw new InvalidOperationException("Error output parameter name cannot be null.");
                    var errorParam = new NpgsqlParameter(errorParamName, NpgsqlDbType.Text)
                    {
                        Direction = ParameterDirection.Output
                    };
                    command.Parameters.Add(errorParam);
                    outputParams[errorParamName] = null;
                }
            }

            if (procedureName == "select_json_entity")
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        result.Load(reader);
                    }
                }
            }
            else
            {
                command.ExecuteNonQuery();
            }

            foreach (NpgsqlParameter param in command.Parameters)
            {
                if (param.Direction == ParameterDirection.Output || param.Direction == ParameterDirection.InputOutput)
                {
                    if (param.ParameterName == "result" && param.Value is string jsonString)
                    {
                        outputParams[param.ParameterName] = JsonDocument.Parse(jsonString).RootElement;
                    }
                    else
                    {
                        outputParams[param.ParameterName] = param.Value == DBNull.Value ? null : param.Value;
                    }
                }
            }

            return (result, outputParams);
        }
    }
    finally
    {
        controlConexion.CerrarBd();
    }
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
            JsonValueKind.Number => jsonElement.TryGetInt32(out var intValue) ? (object)intValue : jsonElement.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
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
