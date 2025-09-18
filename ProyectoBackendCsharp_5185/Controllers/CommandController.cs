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


[Route("api/{projectName}/commands")]
[ApiController]
public class CommandController : ControllerBase
{
    private readonly ControlConexion controlConexion;
    private readonly IConfiguration _configuration;

    public CommandController(ControlConexion controlConexion, IConfiguration configuration)
    {
        this.controlConexion = controlConexion ?? throw new ArgumentNullException(nameof(controlConexion));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }

    [HttpPost("execute")]
    public IActionResult ExecuteCommand(string projectName, [FromBody] Dictionary<string, JsonElement> commandRequest)
    {
        if (commandRequest == null || !commandRequest.ContainsKey("command") || string.IsNullOrWhiteSpace(commandRequest["command"].GetString()))
            return BadRequest("The command cannot be empty.");

        try
        {
            string sqlCommand = commandRequest["command"].GetString()!;
            string provider = _configuration["DatabaseProvider"] ?? throw new InvalidOperationException("DatabaseProvider not configured.");

            DataTable resultado;
            switch (provider)
            {
                case "SqlServer":
                    resultado = ExecutePostgresCommand(sqlCommand, commandRequest);
                    break;
                case "Postgres":
                    resultado = ExecutePostgresCommand(sqlCommand, commandRequest);
                    break;
                case "Oracle":
                    resultado = ExecutePostgresCommand(sqlCommand, commandRequest);
                    break;
                case "MySql":
                    resultado = ExecutePostgresCommand(sqlCommand, commandRequest);
                    break;
                case "LocalDb":
                    resultado = ExecutePostgresCommand(sqlCommand, commandRequest);
                    break;
                default:
                    throw new InvalidOperationException("Unsupported database provider.");
            }

            var response = ConvertDataTableToResponse(resultado);
            return Ok(new { result = response });
        }
        catch (Exception ex)
        {
            return StatusCode(500, $"Internal server error: {ex.Message}. Command: {commandRequest["command"]}");
        }
    }



private DataTable ExecutePostgresCommand(string sqlCommand, Dictionary<string, JsonElement> commandRequest)
{
    controlConexion.AbrirBd();
    try
    {
        var parametros = new List<NpgsqlParameter>();
        var connection = controlConexion.GetConnection() as NpgsqlConnection;
        if (connection == null)
        {
            throw new InvalidOperationException("Invalid connection type for Postgres.");
        }

        using (var command = new NpgsqlCommand(sqlCommand, connection))
        {
            command.CommandType = CommandType.Text;

            if (commandRequest.ContainsKey("parameters"))
            {
                var parameters = commandRequest["parameters"].EnumerateObject();
                foreach (var param in parameters)
                {
                    string paramName = param.Name;
                    var value = ConvertJsonElement(param.Value);

                    // Encriptar el valor si es un campo de contraseña y no es nulo
                    if (IsPasswordField(param.Name) && value != null)
                    {
                        value = BCrypt.Net.BCrypt.HashPassword(value.ToString());
                    }

                    var parameter = new NpgsqlParameter(paramName, value ?? DBNull.Value);
                    command.Parameters.Add(parameter);
                }
            }

            var result = new DataTable();
            using (var reader = command.ExecuteReader())
            {
                result.Load(reader);
            }
            return result;
        }
    }
    finally
    {
        controlConexion.CerrarBd();
    }
}



private bool IsPasswordField(string fieldName)
{
    var passwordKeys = new[] { "password", "contrasena", "passw" };
    return passwordKeys.Any(pk => fieldName.IndexOf(pk, StringComparison.OrdinalIgnoreCase) >= 0);
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
