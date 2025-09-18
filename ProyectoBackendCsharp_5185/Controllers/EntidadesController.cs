#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Data.SqlClient;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using MySql.Data.MySqlClient;
using System.Linq;
using System.Text.Json;
using ProyectoBackendCsharp.Models;
using ProyectoBackendCsharp.Services;
using BCrypt.Net;

namespace ProyectoBackendCsharp.Controllers
{
    [Route("api/{projectName}/{tableName}")]
    [ApiController]
    [Authorize]
    public class DynamicController : ControllerBase
    {
        private readonly ControlConexion controlConexion;
        private readonly IConfiguration _configuration;

        public DynamicController(ControlConexion controlConexion, IConfiguration configuration)
        {
            this.controlConexion = controlConexion ?? throw new ArgumentNullException(nameof(controlConexion));
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        [AllowAnonymous]
        [HttpGet]
        public IActionResult Listar(string projectName, string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                return BadRequest("El nombre de la tabla no puede estar vacío.");

            try
            {
                var lista = new List<Dictionary<string, object?>>();
                string comandoSQL = $"SELECT * FROM {tableName}";

                controlConexion.AbrirBd();
                var tabla = controlConexion.EjecutarConsultaSql(comandoSQL, null);
                controlConexion.CerrarBd();

                foreach (DataRow fila in tabla.Rows)
                {
                    var propiedades = fila.Table.Columns.Cast<DataColumn>()
                                        .ToDictionary(col => col.ColumnName, col => fila[col] == DBNull.Value ? null : fila[col]);
                    lista.Add(propiedades);
                }

                return Ok(lista);
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Error interno del servidor: {ex.Message}");
            }
        }

[AllowAnonymous]
[HttpGet("{keyName}/{value}")]
public IActionResult GetByKey(string projectName, string tableName, string keyName, string value)
{
    if (string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(keyName) || string.IsNullOrWhiteSpace(value))
    {
        return BadRequest("El nombre de la tabla, el nombre de la clave y el valor no pueden estar vacíos.");
    }

    controlConexion.AbrirBd();
    try
    {
        string provider = _configuration["DatabaseProvider"] ?? throw new InvalidOperationException("DatabaseProvider not configured.");
        
        string query;
        DbParameter[] parameters;
        if (provider == "Oracle")
        {
            query = "SELECT DATA_TYPE FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = :tableName AND COLUMN_NAME = :columnName";
            parameters = new DbParameter[]
            {
                CreateParameter(":tableName", tableName.ToUpper()),
                CreateParameter(":columnName", keyName.ToUpper())
            };
        }
        else
        {
            query = "SELECT data_type FROM information_schema.columns WHERE table_name = @tableName AND column_name = @columnName";
            parameters = new DbParameter[]
            {
                CreateParameter("@tableName", tableName),
                CreateParameter("@columnName", keyName)
            };
        }

        Console.WriteLine($"Executing SQL query: {query} with parameters: tableName={tableName.ToUpper()}, columnName={keyName.ToUpper()}");

        var dataTypeResult = controlConexion.EjecutarConsultaSql(query, parameters);

        if (dataTypeResult == null || dataTypeResult.Rows.Count == 0 || dataTypeResult.Rows[0]["DATA_TYPE"] == DBNull.Value)
        {
            return NotFound("No se pudo determinar el tipo de dato.");
        }

        string dataType = dataTypeResult.Rows[0]["DATA_TYPE"]?.ToString() ?? "";
        Console.WriteLine($"Detected data type for column {keyName}: {dataType}");

        if (string.IsNullOrEmpty(dataType))
        {
            return NotFound("No se pudo determinar el tipo de dato.");
        }

        object convertedValue;
        string comandoSQL;

        switch (dataType.ToLower())
        {
            case "int":
            case "bigint":
            case "integer":
                if (int.TryParse(value, out int intValue))
                {
                    convertedValue = intValue;
                    comandoSQL = provider == "Oracle" ? $"SELECT * FROM {tableName} WHERE {keyName} = :Value" 
                                                      : $"SELECT * FROM {tableName} WHERE {keyName} = @Value";
                }
                else
                {
                    return BadRequest("El valor proporcionado no es válido para el tipo de datos entero.");
                }
                break;
            case "number":
            case "decimal":
            case "numeric":
                if (decimal.TryParse(value, out decimal decimalValue))
                {
                    convertedValue = decimalValue;
                    comandoSQL = provider == "Oracle" ? $"SELECT * FROM {tableName} WHERE {keyName} = :Value" 
                                                      : $"SELECT * FROM {tableName} WHERE {keyName} = @Value";
                }
                else
                {
                    return BadRequest("El valor proporcionado no es válido para el tipo de datos número.");
                }
                break;
            case "bit":
            case "boolean":
                if (bool.TryParse(value, out bool boolValue))
                {
                    convertedValue = boolValue;
                    comandoSQL = provider == "Oracle" ? $"SELECT * FROM {tableName} WHERE {keyName} = :Value" 
                                                      : $"SELECT * FROM {tableName} WHERE {keyName} = @Value";
                }
                else
                {
                    return BadRequest("El valor proporcionado no es válido para el tipo de datos booleano.");
                }
                break;
            case "float":
            case "real":
            case "double precision":
                if (double.TryParse(value, out double doubleValue))
                {
                    convertedValue = doubleValue;
                    comandoSQL = provider == "Oracle" ? $"SELECT * FROM {tableName} WHERE {keyName} = :Value" 
                                                      : $"SELECT * FROM {tableName} WHERE {keyName} = @Value";
                }
                else
                {
                    return BadRequest("El valor proporcionado no es válido para el tipo de datos doble.");
                }
                break;
            case "nvarchar":
            case "varchar":
            case "character varying":
            case "text":
            case "char":
            case "varchar2":
                convertedValue = value;
                comandoSQL = provider == "Oracle" ? $"SELECT * FROM {tableName} WHERE {keyName} = :Value" 
                                                  : $"SELECT * FROM {tableName} WHERE {keyName} = @Value";
                break;
            case "date":
            case "datetime":
            case "timestamp":
            case "timestamp with time zone":
            case "timestamp without time zone":
            case "timestamp(6)":
                if (DateTime.TryParse(value, out DateTime dateValue))
                {
                    if (provider == "Postgres")
                    {
                        comandoSQL = $"SELECT * FROM {tableName} WHERE DATE({keyName}) = @Value::date";
                        convertedValue = dateValue.Date.ToString("yyyy-MM-dd");
                    }
                    else if (provider == "Oracle")
                    {
                        comandoSQL = $"SELECT * FROM {tableName} WHERE TRUNC({keyName}) = TRUNC(:Value)";
                        convertedValue = dateValue;
                    }
                    else
                    {
                        comandoSQL = $"SELECT * FROM {tableName} WHERE CAST({keyName} AS DATE) = @Value";
                        convertedValue = dateValue.Date;
                    }
                }
                else
                {
                    return BadRequest("El valor proporcionado no es válido para el tipo de datos fecha.");
                }
                break;
            default:
                return BadRequest($"Tipo de dato no soportado: {dataType}");
        }

        var parametro = CreateParameter(provider == "Oracle" ? ":Value" : "@Value", convertedValue);

        Console.WriteLine($"Executing SQL query: {comandoSQL} with parameter: {parametro.ParameterName} = {parametro.Value}, DbType: {parametro.DbType}");

        var resultado = controlConexion.EjecutarConsultaSql(comandoSQL, new DbParameter[] { parametro });

        Console.WriteLine($"DataSet fill completed for query: {comandoSQL}");

        if (resultado.Rows.Count > 0)
        {
            var lista = new List<Dictionary<string, object?>>();
            foreach (DataRow fila in resultado.Rows)
            {
                var propiedades = resultado.Columns.Cast<DataColumn>()
                                   .ToDictionary(col => col.ColumnName, col => fila[col] == DBNull.Value ? null : fila[col]);
                lista.Add(propiedades);
            }

            return Ok(lista);
        }

        return NotFound();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Exception occurred: {ex.Message}");
        return StatusCode(500, $"Error interno del servidor: {ex.Message}");
    }
    finally
    {
        controlConexion.CerrarBd();
    }
}
        private bool TryParseDate(string value, out DateTime date)
        {
            var formats = new[] { "dd/MM/yyyy", "dd-MM-yyyy", "dd.MM.yyyy", "yyyy/MM/dd", "yyyy-MM-dd", "yyyy.MM.dd", "MM/dd/yyyy", "MM-dd-yyyy", "MM.dd.yyyy" };
            foreach (var format in formats)
            {
                if (DateTime.TryParseExact(value, format, null, System.Globalization.DateTimeStyles.None, out date))
                {
                    return true;
                }
            }
            date = default;
            return false;
        }

[AllowAnonymous] 
[HttpPost] 
public IActionResult Crear(string projectName, string tableName, [FromBody] Dictionary<string, object?> entidadData)  // Line 3
{
    if (string.IsNullOrWhiteSpace(tableName) || entidadData == null || !entidadData.Any())  // Line 4
        return BadRequest("El nombre de la tabla y los datos de la entidad no pueden estar vacíos.");  // Line 5

    try
    {
        var propiedades = entidadData.ToDictionary(  // Line 7
            kvp => kvp.Key,
            kvp => kvp.Value is JsonElement jsonElement ? ConvertJsonElement(jsonElement) : kvp.Value);

        // Case-insensitive check for password fields  // Line 11 (New)
        var passwordKeys = new[] { "password", "contrasena", "passw" };  // Line 12 (New)
        var passwordKey = propiedades.Keys.FirstOrDefault(k => passwordKeys.Any(pk => k.IndexOf(pk, StringComparison.OrdinalIgnoreCase) >= 0));  // Line 13 (New)
        
        if (passwordKey != null)  // Line 15 (New)
        {
            var plainPassword = propiedades[passwordKey]?.ToString();  // Line 16 (New)
            if (!string.IsNullOrEmpty(plainPassword))  // Line 17 (New)
            {
                propiedades[passwordKey] = BCrypt.Net.BCrypt.HashPassword(plainPassword);  // Line 18 (New)
            }
        }

        string provider = _configuration["DatabaseProvider"] ?? throw new InvalidOperationException("DatabaseProvider not configured.");  // Line 21
        var columnas = string.Join(",", propiedades.Keys);  // Line 22
        var valores = string.Join(",", propiedades.Keys.Select(k => $"{GetParameterPrefix(provider)}{k}"));  // Line 23 (Modified)
        string comandoSQL = $"INSERT INTO {tableName} ({columnas}) VALUES ({valores})";  // Line 24

        var parametros = propiedades.Select(p => CreateParameter($"{GetParameterPrefix(provider)}{p.Key}", p.Value)).ToArray();  // Line 26 (Modified)

        Console.WriteLine($"Executing SQL query: {comandoSQL} with parameters:");  // Line 28
        foreach (var parametro in parametros)  // Line 29
        {
            Console.WriteLine($"{parametro.ParameterName} = {parametro.Value}, DbType: {parametro.DbType}");  // Line 30
        }

        controlConexion.AbrirBd();  // Line 32
        controlConexion.EjecutarComandoSql(comandoSQL, parametros);  // Line 33
        controlConexion.CerrarBd();  // Line 34

        return Ok("Entidad creada exitosamente.");  // Line 36
    }
    catch (Exception ex)  // Line 38
    {
        Console.WriteLine($"Exception occurred: {ex.Message}");  // Line 39
        return StatusCode(500, $"Error interno del servidor: {ex.Message}");  // Line 40
    }
}


private object? ConvertJsonElement(JsonElement jsonElement)
{
    if (jsonElement.ValueKind == JsonValueKind.Null)
        return null;

    switch (jsonElement.ValueKind)
    {
        case JsonValueKind.String:
            return DateTime.TryParse(jsonElement.GetString(), out DateTime dateValue) ? (object)dateValue : jsonElement.GetString();
        case JsonValueKind.Number:
            return jsonElement.TryGetInt32(out var intValue) ? (object)intValue : jsonElement.GetDouble();
        case JsonValueKind.True:
            return true;
        case JsonValueKind.False:
            return false;
        case JsonValueKind.Null:
            return null;
        case JsonValueKind.Object:
            return jsonElement.GetRawText();
        case JsonValueKind.Array:
            return jsonElement.GetRawText();
        default:
            throw new InvalidOperationException($"Unsupported JsonValueKind: {jsonElement.ValueKind}");
    }
}


[AllowAnonymous]
[HttpPut("{keyName}/{keyValue}")]
public IActionResult Actualizar(string projectName, string tableName, string keyName, string keyValue, [FromBody] Dictionary<string, object?> entidadData)
{
    if (string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(keyName) || entidadData == null || !entidadData.Any())
        return BadRequest("El nombre de la tabla, el nombre de la clave y los datos de la entidad no pueden estar vacíos.");

    try
    {
        var propiedades = entidadData.ToDictionary(
            kvp => kvp.Key,
            kvp => kvp.Value is JsonElement jsonElement ? ConvertJsonElement(jsonElement) : kvp.Value);

        // Case-insensitive check for password fields
        var passwordKeys = new[] { "password", "contrasena", "passw" };
        var passwordKey = propiedades.Keys.FirstOrDefault(k => passwordKeys.Any(pk => k.IndexOf(pk, StringComparison.OrdinalIgnoreCase) >= 0));
        
        if (passwordKey != null)
        {
            var plainPassword = propiedades[passwordKey]?.ToString();
            if (!string.IsNullOrEmpty(plainPassword))
            {
                propiedades[passwordKey] = BCrypt.Net.BCrypt.HashPassword(plainPassword);
            }
        }

        string provider = _configuration["DatabaseProvider"] ?? throw new InvalidOperationException("DatabaseProvider not configured.");
        var actualizaciones = string.Join(",", propiedades.Select(p => $"{p.Key}={GetParameterPrefix(provider)}{p.Key}"));
        string comandoSQL = $"UPDATE {tableName} SET {actualizaciones} WHERE {keyName}={GetParameterPrefix(provider)}KeyValue";

        var parametros = propiedades.Select(p => CreateParameter($"{GetParameterPrefix(provider)}{p.Key}", p.Value)).ToList();
        parametros.Add(CreateParameter($"{GetParameterPrefix(provider)}KeyValue", keyValue));

        Console.WriteLine($"Executing SQL query: {comandoSQL} with parameters:");
        foreach (var parametro in parametros)
        {
            Console.WriteLine($"{parametro.ParameterName} = {parametro.Value}, DbType: {parametro.DbType}");
        }

        controlConexion.AbrirBd();
        controlConexion.EjecutarComandoSql(comandoSQL, parametros.ToArray());
        controlConexion.CerrarBd();

        return Ok("Entidad actualizada exitosamente.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Exception occurred: {ex.Message}");
        return StatusCode(500, $"Error interno del servidor: {ex.Message}");
    }
}

private string GetParameterPrefix(string provider)
{
    return provider == "Oracle" ? ":" : "@";
}
        [AllowAnonymous]
        [HttpDelete("{keyName}/{keyValue}")]
        public IActionResult Eliminar(string projectName, string tableName, string keyName, string keyValue)
        {
            if (string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(keyName))
                return BadRequest("El nombre de la tabla o el nombre de la clave no pueden estar vacíos.");

            try
            {
                string provider = _configuration["DatabaseProvider"] ?? throw new InvalidOperationException("DatabaseProvider not configured.");
                string comandoSQL = provider == "Oracle" ? $"DELETE FROM {tableName} WHERE {keyName}=:KeyValue" : $"DELETE FROM {tableName} WHERE {keyName}=@KeyValue";
                var parametro = CreateParameter(provider == "Oracle" ? ":KeyValue" : "@KeyValue", keyValue);

                controlConexion.AbrirBd();
                controlConexion.EjecutarComandoSql(comandoSQL, new[] { parametro });
                controlConexion.CerrarBd();

                return Ok("Entidad eliminada exitosamente.");
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Error interno del servidor: {ex.Message}");
            }
        }

        [AllowAnonymous]
        [HttpGet("/")]
        public IActionResult GetRoot()
        {
            return Ok("API is running");
        }

[AllowAnonymous]
[HttpPost("verificar-contrasena")]
public IActionResult VerificarContrasena(string projectName, string tableName, [FromBody] Dictionary<string, string> datos)
{
    if (string.IsNullOrWhiteSpace(tableName) || datos == null || !datos.ContainsKey("userField") || !datos.ContainsKey("passwordField") || !datos.ContainsKey("userValue") || !datos.ContainsKey("passwordValue"))
        return BadRequest("El nombre de la tabla, el campo de usuario, el campo de contraseña, el valor de usuario y el valor de contraseña no pueden estar vacíos.");

    try
    {
        string userField = datos["userField"];
        string passwordField = datos["passwordField"];
        string userValue = datos["userValue"];
        string passwordValue = datos["passwordValue"];

        string provider = _configuration["DatabaseProvider"] ?? throw new InvalidOperationException("DatabaseProvider not configured.");
        string comandoSQL;
        DbParameter parametro;

        if (provider == "Oracle")
        {
            comandoSQL = $"SELECT {passwordField} FROM {tableName} WHERE {userField} = :UserValue";
            parametro = CreateParameter(":UserValue", userValue);
        }
        else
        {
            comandoSQL = $"SELECT {passwordField} FROM {tableName} WHERE {userField} = @UserValue";
            parametro = CreateParameter("@UserValue", userValue);
        }

        controlConexion.AbrirBd();
        var resultado = controlConexion.EjecutarConsultaSql(comandoSQL, new DbParameter[] { parametro });
        controlConexion.CerrarBd();

        if (resultado.Rows.Count == 0)
        {
            return NotFound("Usuario no encontrado.");
        }

        string hashedPassword = resultado.Rows[0][passwordField]?.ToString() ?? string.Empty;

        // Add logging to check the hash
        Console.WriteLine($"Hashed password from database: {hashedPassword}");

        if (!hashedPassword.StartsWith("$2"))
        {
            throw new InvalidOperationException("Stored password hash is not a valid BCrypt hash.");
        }

        bool isPasswordValid = BCrypt.Net.BCrypt.Verify(passwordValue, hashedPassword);

        if (isPasswordValid)
        {
            return Ok("Contraseña verificada exitosamente.");
        }
        else
        {
            return Unauthorized("Contraseña incorrecta.");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Exception occurred: {ex.Message}");
        return StatusCode(500, $"Error interno del servidor: {ex.Message}");
    }
}

        public DbParameter CreateParameter(string name, object? value)
        {
            string provider = _configuration["DatabaseProvider"] ?? throw new InvalidOperationException("DatabaseProvider not configured.");
            return provider switch
            {
                "SqlServer" => new SqlParameter(name, value ?? DBNull.Value),
                "LocalDb" => new SqlParameter(name, value ?? DBNull.Value),
                "Postgres" => new NpgsqlParameter(name, value ?? DBNull.Value),
                "Oracle" => new OracleParameter(name, value ?? DBNull.Value),
                "MySql" => new MySqlParameter(name, value ?? DBNull.Value),
                _ => throw new InvalidOperationException("Unsupported database provider."),
            };
        }
    }
}
