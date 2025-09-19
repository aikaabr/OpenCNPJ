using System.Text.Json;

namespace CNPJExporter.Configuration;

public class AppConfig
{
    public PathsConfig Paths { get; set; } = new();
    public RcloneSettings Rclone { get; set; } = new();
    public StorageSettings Storage { get; set; } = new();
    public DuckDbSettings DuckDb { get; set; } = new();
    public NdjsonSettings Ndjson { get; set; } = new();
    public DownloaderSettings Downloader { get; set; } = new();

    public class PathsConfig
    {
        public string DataDir { get; set; } = string.Empty;
        public string ParquetDir { get; set; } = string.Empty;
        public string OutputDir { get; set; } = string.Empty;
        public string DownloadDir { get; set; } = string.Empty;
        public string HashCacheDir { get; set; } = string.Empty;

        /// <summary>
        /// Aplica os valores padrão baseados nas configurações de ambiente
        /// </summary>
        public void ApplyEnvironmentDefaults()
        {
            if (string.IsNullOrEmpty(DataDir))
                DataDir = EnvironmentPathsConfig.ExtractedDataPath;

            if (string.IsNullOrEmpty(ParquetDir))
                ParquetDir = EnvironmentPathsConfig.ParquetDataPath;

            if (string.IsNullOrEmpty(OutputDir))
                OutputDir = EnvironmentPathsConfig.OutputPath;

            if (string.IsNullOrEmpty(DownloadDir))
                DownloadDir = EnvironmentPathsConfig.DownloadPath;

            if (string.IsNullOrEmpty(HashCacheDir))
                HashCacheDir = EnvironmentPathsConfig.HashCachePath;
        }
    }

    public class RcloneSettings
    {
        public string RemoteBase { get; set; } = string.Empty;
        public int Transfers { get; set; } = 0;
        public int MaxConcurrentUploads { get; set; } = 0;

        /// <summary>
        /// Aplica os valores padrão baseados nas configurações de ambiente
        /// </summary>
        public void ApplyEnvironmentDefaults()
        {
            RemoteBase = Environment.GetEnvironmentVariable("RCLONE_REMOTE") ?? RemoteBase;

            if (int.TryParse(Environment.GetEnvironmentVariable("RCLONE_TRANSFERS"), out var transfers))
                Transfers = transfers;

            if (int.TryParse(Environment.GetEnvironmentVariable("RCLONE_MAX_CONCURRENT"), out var maxConcurrent))
                MaxConcurrentUploads = maxConcurrent;
        }
    }

    public class StorageSettings
    {
        public string Type { get; set; } = "rclone"; // "rclone", "filesystem", "s3"
        public bool Enabled { get; set; } = true;
        public FileSystemStorageSettings FileSystem { get; set; } = new();
        public S3StorageSettings S3 { get; set; } = new();

        /// <summary>
        /// Aplica os valores padrão baseados nas configurações de ambiente
        /// </summary>
        public void ApplyEnvironmentDefaults()
        {
            Type = Environment.GetEnvironmentVariable("STORAGE_TYPE") ?? Type;

            if (bool.TryParse(Environment.GetEnvironmentVariable("STORAGE_ENABLED"), out var enabled))
                Enabled = enabled;

            FileSystem.ApplyEnvironmentDefaults();
            S3.ApplyEnvironmentDefaults();
        }
    }

    public class FileSystemStorageSettings
    {
        public string OutputPath { get; set; } = "./output";

        /// <summary>
        /// Aplica os valores padrão baseados nas configurações de ambiente
        /// </summary>
        public void ApplyEnvironmentDefaults()
        {
            OutputPath = Environment.GetEnvironmentVariable("FILESYSTEM_OUTPUT_PATH") ?? EnvironmentPathsConfig.OutputPath;
        }
    }

    public class S3StorageSettings
    {
        public string BucketName { get; set; } = string.Empty;
        public string Region { get; set; } = string.Empty;
        public string AccessKey { get; set; } = string.Empty;
        public string SecretKey { get; set; } = string.Empty;
        public string Prefix { get; set; } = string.Empty;

        /// <summary>
        /// Aplica os valores padrão baseados nas configurações de ambiente
        /// </summary>
        public void ApplyEnvironmentDefaults()
        {
            BucketName = Environment.GetEnvironmentVariable("S3_BUCKET_NAME") ?? BucketName;
            Region = Environment.GetEnvironmentVariable("S3_REGION") ?? Region;
            AccessKey = Environment.GetEnvironmentVariable("S3_ACCESS_KEY") ?? AccessKey;
            SecretKey = Environment.GetEnvironmentVariable("S3_SECRET_KEY") ?? SecretKey;
            Prefix = Environment.GetEnvironmentVariable("S3_PREFIX") ?? Prefix;
        }
    }

    public class DuckDbSettings
    {
        public bool UseInMemory { get; set; } = false;
        public int ThreadsPragma { get; set; } = 0;
        public string MemoryLimit { get; set; } = string.Empty;
        public int EngineThreads { get; set; } = 0;
        public bool PreserveInsertionOrder { get; set; } = false;

        /// <summary>
        /// Aplica os valores padrão baseados nas configurações de ambiente
        /// </summary>
        public void ApplyEnvironmentDefaults()
        {
            if (bool.TryParse(Environment.GetEnvironmentVariable("DUCKDB_IN_MEMORY"), out var inMemory))
                UseInMemory = inMemory;

            if (int.TryParse(Environment.GetEnvironmentVariable("DUCKDB_THREADS"), out var threads))
                ThreadsPragma = threads;

            MemoryLimit = Environment.GetEnvironmentVariable("DUCKDB_MEMORY_LIMIT") ?? MemoryLimit;

            if (int.TryParse(Environment.GetEnvironmentVariable("DUCKDB_ENGINE_THREADS"), out var engineThreads))
                EngineThreads = engineThreads;

            if (bool.TryParse(Environment.GetEnvironmentVariable("DUCKDB_PRESERVE_ORDER"), out var preserveOrder))
                PreserveInsertionOrder = preserveOrder;
        }
    }

    public class NdjsonSettings
    {
        public int BatchUploadSize { get; set; } = 0;
        public bool NormalizeBeforeHash { get; set; } = false;
        public bool WriteJsonFiles { get; set; } = false;
        public int MaxParallelProcessing { get; set; } = 0;

        /// <summary>
        /// Aplica os valores padrão baseados nas configurações de ambiente
        /// </summary>
        public void ApplyEnvironmentDefaults()
        {
            if (int.TryParse(Environment.GetEnvironmentVariable("NDJSON_BATCH_SIZE"), out var batchSize))
                BatchUploadSize = batchSize;

            if (bool.TryParse(Environment.GetEnvironmentVariable("NDJSON_NORMALIZE"), out var normalize))
                NormalizeBeforeHash = normalize;

            if (bool.TryParse(Environment.GetEnvironmentVariable("NDJSON_WRITE_FILES"), out var writeFiles))
                WriteJsonFiles = writeFiles;

            if (int.TryParse(Environment.GetEnvironmentVariable("NDJSON_MAX_PARALLEL"), out var maxParallel))
                MaxParallelProcessing = maxParallel;
        }
    }

    public class DownloaderSettings
    {
        public int ParallelDownloads { get; set; } = 0;

        /// <summary>
        /// Aplica os valores padrão baseados nas configurações de ambiente
        /// </summary>
        public void ApplyEnvironmentDefaults()
        {
            if (int.TryParse(Environment.GetEnvironmentVariable("DOWNLOADER_PARALLEL"), out var parallel))
                ParallelDownloads = parallel;
        }
    }

    public static AppConfig Current { get; private set; } = new();

    public static AppConfig Load(string? path = null)
    {
        var configPath = path ?? Path.Combine(Environment.CurrentDirectory, "config.json");
        try
        {
            if (File.Exists(configPath))
            {
                var json = File.ReadAllText(configPath);
                var cfg = JsonSerializer.Deserialize<AppConfig>(json, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                });
                if (cfg != null)
                {
                    Current = cfg;
                    // Aplicar configurações de ambiente após carregar do JSON
                    Current.ApplyEnvironmentDefaults();
                    return Current;
                }
            }
        }
        catch
        {
        }

        Current = new();
        // Aplicar configurações de ambiente para configuração padrão
        Current.ApplyEnvironmentDefaults();
        return Current;
    }

    /// <summary>
    /// Aplica valores padrão baseados em variáveis de ambiente para todas as seções de configuração
    /// </summary>
    public void ApplyEnvironmentDefaults()
    {
        Paths.ApplyEnvironmentDefaults();
        Storage.ApplyEnvironmentDefaults();
        Rclone.ApplyEnvironmentDefaults();
        DuckDb.ApplyEnvironmentDefaults();
        Ndjson.ApplyEnvironmentDefaults();
        Downloader.ApplyEnvironmentDefaults();
    }
}
