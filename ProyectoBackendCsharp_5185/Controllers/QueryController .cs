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

namespace ProyectoBackendCsharp.Controllers
{
    [Route("api/{projectName}/queries")]
    [ApiController]
    public class QueryController : ControllerBase
    {
        private readonly ControlConexion controlConexion;
        private readonly IConfiguration _configuration;

        public QueryController(ControlConexion controlConexion, IConfiguration configuration)
        {
            this.controlConexion = controlConexion ?? throw new ArgumentNullException(nameof(controlConexion));
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        // Método para verificar si un campo es una contraseña
        private bool IsPasswordField(string fieldName)
        {
            var passwordKeys = new[] { "password", "contrasena", "passw" };
            return passwordKeys.Any(pk => fieldName.IndexOf(pk, StringComparison.OrdinalIgnoreCase) >= 0);
        }

        // Método para verificar si una cadena es un JSON válido
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

        [HttpPost("execute")]
        public IActionResult ExecuteQuery(string projectName, [FromBody] Dictionary<string, JsonElement> queryRequest)
        {
            if (queryRequest == null || !queryRequest.ContainsKey("query") || string.IsNullOrWhiteSpace(queryRequest["query"].GetString()))
                return BadRequest("The query cannot be empty.");

            DataTable resultado = new DataTable();
            Dictionary<string, object?> outputParams = new Dictionary<string, object?>();

            try
            {
                string sqlQuery = queryRequest["query"].GetString()!;
                string provider = _configuration["DatabaseProvider"] ?? throw new InvalidOperationException("DatabaseProvider not configured.");

                switch (provider)
                {
                    case "SqlServer":
                        (resultado, outputParams) = ExecutePostgresQuery(sqlQuery, queryRequest);
                        break;
                    case "Postgres":
                        (resultado, outputParams) = ExecutePostgresQuery(sqlQuery, queryRequest);
                        break;
                    case "Oracle":
                        (resultado, outputParams) = ExecutePostgresQuery(sqlQuery, queryRequest);
                        break;
                    case "MySql":
                        (resultado, outputParams) = ExecutePostgresQuery(sqlQuery, queryRequest);
                        break;
                    case "LocalDb":
                        (resultado, outputParams) = ExecutePostgresQuery(sqlQuery, queryRequest);
                        break;
                    default:
                        throw new InvalidOperationException("Unsupported database provider.");
                }

                var response = ConvertDataTableToResponse(resultado);

                return Ok(new { result = response, outputParams = outputParams });
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Internal server error: {ex.Message}. Query: {queryRequest["query"]}");
            }
        }
        private (DataTable, Dictionary<string, object?>) ExecutePostgresQuery(string sqlQuery, Dictionary<string, JsonElement> queryRequest)
        {
            controlConexion.AbrirBd();
            try
            {
                var parametros = new List<NpgsqlParameter>();
                var outputParams = new Dictionary<string, object?>();

                var connection = controlConexion.GetConnection() as NpgsqlConnection;
                if (connection == null)
                {
                    throw new InvalidOperationException("Invalid connection type for Postgres.");
                }

                bool isExplicitCall = sqlQuery.Trim().ToUpper().StartsWith("CALL");
                bool isFunction = sqlQuery.Trim().ToUpper().StartsWith("SELECT") && sqlQuery.Contains("(") && sqlQuery.Contains(")");
                bool isImplicitProcedure = !isExplicitCall && !isFunction && !sqlQuery.Trim().ToUpper().StartsWith("SELECT");

                using (var command = new NpgsqlCommand())
                {
                    command.Connection = connection;

                    if (isExplicitCall)
                    {
                        command.CommandText = sqlQuery;
                        command.CommandType = CommandType.Text;
                    }
                    else if (isImplicitProcedure)
                    {
                        command.CommandText = sqlQuery;
                        command.CommandType = CommandType.StoredProcedure;
                    }
                    else
                    {
                        command.CommandText = sqlQuery;
                        command.CommandType = CommandType.Text;
                    }

                    if (queryRequest.ContainsKey("parameters"))
                    {
                        var parameters = queryRequest["parameters"].EnumerateObject();
                        foreach (var param in parameters)
                        {
                            string paramName = param.Name.Trim();
                            var value = param.Value.ValueKind == JsonValueKind.String ? param.Value.GetString() : param.Value.GetRawText();

                            // Verificar que value no sea nulo antes de pasar a IsValidJson
                            if (!string.IsNullOrEmpty(value) && IsValidJson(value))
                            {
                                var jsonDoc = JsonDocument.Parse(value);
                                var jsonObject = jsonDoc.RootElement.Clone();
                                EncriptarPasswordEnJson(ref jsonObject);
                                value = jsonObject.GetRawText();
                            }
                            else if (IsPasswordField(paramName))
                            {
                                if (!string.IsNullOrEmpty(value))
                                {
                                    value = BCrypt.Net.BCrypt.HashPassword(value);
                                }
                            }

                            var parameter = new NpgsqlParameter(paramName, value);
                            if (sqlQuery.Contains($"@{paramName}::jsonb") || (value is string stringValue && IsValidJson(stringValue)))
                            {
                                parameter.NpgsqlDbType = NpgsqlDbType.Jsonb;
                            }
                            command.Parameters.Add(parameter);
                        }
                    }

                    if (queryRequest.ContainsKey("outputParameter"))
                    {
                        string outputParamName = queryRequest["outputParameter"].GetString()!;
                        var outputParam = new NpgsqlParameter(outputParamName, NpgsqlDbType.Text)
                        {
                            Direction = ParameterDirection.Output
                        };
                        command.Parameters.Add(outputParam);
                    }

                    if (queryRequest.ContainsKey("errorOutputParameter"))
                    {
                        string errorOutputParamName = queryRequest["errorOutputParameter"].GetString()!;
                        var errorOutputParam = new NpgsqlParameter(errorOutputParamName, NpgsqlDbType.Text)
                        {
                            Direction = ParameterDirection.Output
                        };
                        command.Parameters.Add(errorOutputParam);
                    }

                    var result = new DataTable();
                    if (isImplicitProcedure || isExplicitCall)
                    {
                        command.ExecuteNonQuery();
                    }
                    else
                    {
                        using (var reader = command.ExecuteReader())
                        {
                            result.Load(reader);
                        }
                    }

                    foreach (NpgsqlParameter param in command.Parameters)
                    {
                        if (param.Direction == ParameterDirection.Output || param.Direction == ParameterDirection.InputOutput)
                        {
                            outputParams[param.ParameterName] = param.Value;
                        }
                    }

                    if (isFunction && result.Rows.Count > 0 && result.Columns.Count > 0)
                    {
                        outputParams["FunctionResult"] = result.Rows[0][0];
                    }

                    return (result, outputParams);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to execute SQL query. Error: {ex.Message}", ex);
            }
            finally
            {
                controlConexion.CerrarBd();
            }
        }

        // Método para encriptar contraseñas dentro de JSON (Modificado: 14/07/2023)
        private void EncriptarPasswordEnJson(ref JsonElement jsonObject)
        {
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
                            EncriptarPasswordEnJson(ref subElement);
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
    }
}
