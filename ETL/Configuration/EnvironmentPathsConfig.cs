using System;
using System.IO;

namespace CNPJExporter.Configuration;

/// <summary>
/// Configuração de paths baseada em variáveis de ambiente
/// Permite centralizar e padronizar todos os caminhos usados pelo sistema
/// </summary>
public static class EnvironmentPathsConfig
{
    private static string? _baseDataPath;

    /// <summary>
    /// Diretório base para todos os dados do projeto
    /// Configurado através da variável de ambiente OPENCNPJ_BASE_PATH
    /// </summary>
    public static string BaseDataPath
    {
        get
        {
            if (_baseDataPath == null)
            {
                _baseDataPath = Environment.GetEnvironmentVariable("OPENCNPJ_BASE_PATH")
                    ?? Path.Combine(Environment.CurrentDirectory, "opencnpj_data");

                // Garantir que o diretório base existe
                Directory.CreateDirectory(_baseDataPath);
            }
            return _baseDataPath;
        }
    }

    /// <summary>
    /// Diretório para downloads temporários
    /// </summary>
    public static string DownloadPath => GetOrCreatePath("OPENCNPJ_DOWNLOAD_PATH", "downloads");

    /// <summary>
    /// Diretório para dados extraídos
    /// </summary>
    public static string ExtractedDataPath => GetOrCreatePath("OPENCNPJ_EXTRACTED_DATA_PATH", "extracted_data");

    /// <summary>
    /// Diretório para arquivos Parquet
    /// </summary>
    public static string ParquetDataPath => GetOrCreatePath("OPENCNPJ_PARQUET_DATA_PATH", "parquet_data");

    /// <summary>
    /// Diretório para arquivos NDJSON de saída
    /// </summary>
    public static string OutputPath => GetOrCreatePath("OPENCNPJ_OUTPUT_PATH", "output");

    /// <summary>
    /// Diretório para cache de hashes
    /// </summary>
    public static string HashCachePath => GetOrCreatePath("OPENCNPJ_HASH_CACHE_PATH", "hash_cache");

    /// <summary>
    /// Diretório temporário para processamento
    /// </summary>
    public static string TempPath => GetOrCreatePath("OPENCNPJ_TEMP_PATH", "temp");

    /// <summary>
    /// Diretório para logs
    /// </summary>
    public static string LogsPath => GetOrCreatePath("OPENCNPJ_LOGS_PATH", "logs");

    /// <summary>
    /// Diretório para backups
    /// </summary>
    public static string BackupPath => GetOrCreatePath("OPENCNPJ_BACKUP_PATH", "backups");

    /// <summary>
    /// Diretório para arquivos de configuração específicos do ambiente
    /// </summary>
    public static string ConfigPath => GetOrCreatePath("OPENCNPJ_CONFIG_PATH", "config");

    /// <summary>
    /// Obtém ou cria um caminho baseado em variável de ambiente ou usa o padrão relativo ao BaseDataPath
    /// </summary>
    /// <param name="envVarName">Nome da variável de ambiente</param>
    /// <param name="defaultSubPath">Subdiretório padrão dentro do BaseDataPath</param>
    /// <returns>Caminho absoluto do diretório</returns>
    private static string GetOrCreatePath(string envVarName, string defaultSubPath)
    {
        var path = Environment.GetEnvironmentVariable(envVarName);

        if (string.IsNullOrEmpty(path))
        {
            path = Path.Combine(BaseDataPath, defaultSubPath);
        }

        // Garantir que o diretório existe
        Directory.CreateDirectory(path);

        return path;
    }

    /// <summary>
    /// Limpa os caches de paths, forçando recarregamento das variáveis de ambiente
    /// </summary>
    public static void RefreshPaths()
    {
        _baseDataPath = null;
    }

    /// <summary>
    /// Retorna um resumo de todos os paths configurados
    /// </summary>
    public static Dictionary<string, string> GetAllPaths()
    {
        return new Dictionary<string, string>
        {
            { "BaseDataPath", BaseDataPath },
            { "DownloadPath", DownloadPath },
            { "ExtractedDataPath", ExtractedDataPath },
            { "ParquetDataPath", ParquetDataPath },
            { "OutputPath", OutputPath },
            { "HashCachePath", HashCachePath },
            { "TempPath", TempPath },
            { "LogsPath", LogsPath },
            { "BackupPath", BackupPath },
            { "ConfigPath", ConfigPath }
        };
    }

    /// <summary>
    /// Valida se todos os diretórios são acessíveis para escrita
    /// </summary>
    public static bool ValidateWriteAccess()
    {
        var paths = GetAllPaths();

        foreach (var (name, path) in paths)
        {
            try
            {
                var testFile = Path.Combine(path, $".write_test_{Guid.NewGuid()}");
                File.WriteAllText(testFile, "test");
                File.Delete(testFile);
            }
            catch
            {
                Console.WriteLine($"Erro: Não é possível escrever no diretório {name}: {path}");
                return false;
            }
        }

        return true;
    }
}