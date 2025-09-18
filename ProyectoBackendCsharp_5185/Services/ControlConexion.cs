#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using MySql.Data.MySqlClient;
using MongoDB.Driver;
using System.IO;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;

namespace ProyectoBackendCsharp.Services
{
    public class ControlConexion
    {
        private readonly IWebHostEnvironment _env;
        private readonly IConfiguration _configuration;
        private IDbConnection? _dbConnection;
        private IMongoDatabase? _mongoDatabase;

        public ControlConexion(IWebHostEnvironment env, IConfiguration configuration)
        {
            _env = env ?? throw new ArgumentNullException(nameof(env));
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            _dbConnection = null;
            _mongoDatabase = null;
        }

        public void AbrirBd()
        {
            try
            {
                string provider = _configuration["DatabaseProvider"] ?? throw new InvalidOperationException("DatabaseProvider not configured.");
                string? connectionString = _configuration.GetConnectionString(provider);

                if (string.IsNullOrEmpty(connectionString))
                    throw new InvalidOperationException("Connection string is null or empty.");

                switch (provider)
                {
                    case "LocalDb":
                        string appDataPath = Path.Combine(_env.ContentRootPath, "App_Data");
                        AppDomain.CurrentDomain.SetData("DataDirectory", appDataPath);
                        _dbConnection = new SqlConnection(connectionString);
                        _dbConnection.Open();
                        break;
                    case "SqlServer":
                        _dbConnection = new SqlConnection(connectionString);
                        _dbConnection.Open();
                        break;
                    case "Postgres":
                        _dbConnection = new NpgsqlConnection(connectionString);
                        _dbConnection.Open();
                        break;
                    case "Oracle":
                        _dbConnection = new OracleConnection(connectionString);
                        _dbConnection.Open();
                        break;
                    case "MySql":
                        _dbConnection = new MySqlConnection(connectionString);
                        _dbConnection.Open();
                        break;
                    case "MongoDb":
                        var client = new MongoClient(connectionString);
                        _mongoDatabase = client.GetDatabase(_configuration["MongoDatabaseName"]);
                        break;
                    default:
                        throw new InvalidOperationException("Unsupported database provider.");
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Failed to open the database connection.", ex);
            }
        }

        public void AbrirBdLocalDB(string archivoDb)
        {
            try
            {
                string dbFileName = archivoDb.EndsWith(".mdf") ? archivoDb : archivoDb + ".mdf";
                string appDataPath = Path.Combine(_env.ContentRootPath, "App_Data");
                string filePath = Path.Combine(appDataPath, dbFileName);

                string connectionString = $@"Data Source=(LocalDB)\MSSQLLocalDB;AttachDbFilename={filePath};Integrated Security=True";
                
                _dbConnection = new SqlConnection(connectionString);
                _dbConnection.Open();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Failed to open the LocalDB connection.", ex);
            }
        }

        public void CerrarBd()
        {
            try
            {
                if (_dbConnection != null && _dbConnection.State == ConnectionState.Open)
                {
                    _dbConnection.Close();
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Failed to close the database connection.", ex);
            }
        }

        public int EjecutarComandoSql(string consultaSql, DbParameter[] parametros)
{
    try
    {
        if (_dbConnection == null || _dbConnection.State != ConnectionState.Open)
            throw new InvalidOperationException("Database connection is not open.");

        using (var comando = _dbConnection.CreateCommand())
        {
            comando.CommandText = consultaSql;
            foreach (var parametro in parametros)
            {
                Console.WriteLine($"Adding parameter: {parametro.ParameterName} = {parametro.Value}, DbType: {parametro.DbType}");
                comando.Parameters.Add(parametro);
            }
            int filasAfectadas = comando.ExecuteNonQuery();
            return filasAfectadas;
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Exception occurred: {ex.Message}");
        throw new InvalidOperationException("Failed to execute SQL command.", ex);
    }
}

        public DataTable EjecutarConsultaSql(string consultaSql, DbParameter[]? parametros)
{
    if (_dbConnection == null || _dbConnection.State != ConnectionState.Open)
        throw new InvalidOperationException("Database connection is not open.");

    try
    {
        using (var comando = _dbConnection.CreateCommand())
        {
            comando.CommandText = consultaSql;
            if (parametros != null)
            {
                foreach (var param in parametros)
                {
                    Console.WriteLine($"Adding parameter: {param.ParameterName} = {param.Value}, DbType: {param.DbType}");
                    comando.Parameters.Add(param);
                }
            }

            var resultado = new DataSet();
            var adaptador = CreateDataAdapter(comando);

            Console.WriteLine($"Executing command: {comando.CommandText}");
            adaptador.Fill(resultado); // Llenar el DataSet
            Console.WriteLine("DataSet filled");

            if (resultado.Tables.Count == 0)
            {
                Console.WriteLine("No tables returned in the DataSet");
                throw new Exception("No tables returned in the DataSet");
            }

            Console.WriteLine($"Number of tables in DataSet: {resultado.Tables.Count}");
            Console.WriteLine($"Number of rows in first table: {resultado.Tables[0].Rows.Count}");

            return resultado.Tables[0]; // Retornar la primera tabla del DataSet
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Exception occurred: {ex.Message}");
        throw new Exception($"Failed to execute SQL query. Error: {ex.Message}", ex);
    }
}

        private IDataAdapter CreateDataAdapter(IDbCommand comando)
        {
            try
            {
                switch (_dbConnection)
                {
                    case SqlConnection:
                        return new SqlDataAdapter((SqlCommand)comando);
                    case NpgsqlConnection:
                        return new NpgsqlDataAdapter((NpgsqlCommand)comando);
                    case OracleConnection:
                        return new OracleDataAdapter((OracleCommand)comando);
                    case MySqlConnection:
                        return new MySqlDataAdapter((MySqlCommand)comando);
                    default:
                        throw new InvalidOperationException("Unsupported database provider.");
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Failed to create data adapter.", ex);
            }
        }

        public IMongoCollection<T> ObtenerColeccionMongoDb<T>(string nombreColeccion)
        {
            try
            {
                if (_mongoDatabase == null)
                    throw new InvalidOperationException("MongoDB connection is not open.");

                return _mongoDatabase.GetCollection<T>(nombreColeccion);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Failed to get MongoDB collection.", ex);
            }
        }

        public List<T> EjecutarConsultaMongoDb<T>(FilterDefinition<T> filtro, string nombreColeccion)
        {
            try
            {
                var coleccion = ObtenerColeccionMongoDb<T>(nombreColeccion);
                return coleccion.Find(filtro).ToList();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Failed to execute MongoDB query.", ex);
            }
        }

        // Método CreateParameter para crear parámetros de consulta
        public DbParameter CreateParameter(string name, object? value)
        {
            try
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
            catch (Exception ex)
            {
                throw new InvalidOperationException("Failed to create parameter.", ex);
            }
        }

        // Nuevo método para obtener la conexión actual
        public IDbConnection? GetConnection()
        {
            return _dbConnection;
        }
    }
}
