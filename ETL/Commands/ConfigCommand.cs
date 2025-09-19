using CNPJExporter.Configuration;
using Spectre.Console;
using Spectre.Console.Cli;

namespace CNPJExporter.Commands;

public class ConfigCommand : Command
{
    public override int Execute(CommandContext context)
    {
        AnsiConsole.MarkupLine("[bold blue]🔧 Configuração OpenCNPJ[/]");
        AnsiConsole.WriteLine();

        // Validar acesso de escrita aos diretórios
        AnsiConsole.MarkupLine("[bold yellow]Validando acesso aos diretórios...[/]");

        if (EnvironmentPathsConfig.ValidateWriteAccess())
        {
            AnsiConsole.MarkupLine("[bold green]✅ Todos os diretórios são acessíveis para escrita[/]");
        }
        else
        {
            AnsiConsole.MarkupLine("[bold red]❌ Alguns diretórios não são acessíveis para escrita[/]");
        }

        AnsiConsole.WriteLine();

        // Exibir configuração de paths
        var pathsTable = new Table()
            .Title("[bold]Configuração de Paths[/]")
            .AddColumn("[bold]Variável de Ambiente[/]")
            .AddColumn("[bold]Path Configurado[/]")
            .AddColumn("[bold]Existe[/]");

        var paths = new[]
        {
            ("OPENCNPJ_BASE_PATH", EnvironmentPathsConfig.BaseDataPath),
            ("OPENCNPJ_DOWNLOAD_PATH", EnvironmentPathsConfig.DownloadPath),
            ("OPENCNPJ_EXTRACTED_DATA_PATH", EnvironmentPathsConfig.ExtractedDataPath),
            ("OPENCNPJ_PARQUET_DATA_PATH", EnvironmentPathsConfig.ParquetDataPath),
            ("OPENCNPJ_OUTPUT_PATH", EnvironmentPathsConfig.OutputPath),
            ("OPENCNPJ_HASH_CACHE_PATH", EnvironmentPathsConfig.HashCachePath),
            ("OPENCNPJ_TEMP_PATH", EnvironmentPathsConfig.TempPath),
            ("OPENCNPJ_LOGS_PATH", EnvironmentPathsConfig.LogsPath),
            ("OPENCNPJ_BACKUP_PATH", EnvironmentPathsConfig.BackupPath),
            ("OPENCNPJ_CONFIG_PATH", EnvironmentPathsConfig.ConfigPath)
        };

        foreach (var (envVar, path) in paths)
        {
            var exists = Directory.Exists(path);
            var envValue = Environment.GetEnvironmentVariable(envVar);
            var pathDisplay = envValue != null ? $"{path} [dim](from env)[/]" : $"{path} [dim](default)[/]";

            pathsTable.AddRow(
                envVar,
                pathDisplay,
                exists ? "[green]✅[/]" : "[red]❌[/]"
            );
        }

        AnsiConsole.Write(pathsTable);
        AnsiConsole.WriteLine();

        // Exibir configuração do AppConfig atual
        var config = AppConfig.Current;
        var configTable = new Table()
            .Title("[bold]Configuração Atual (AppConfig)[/]")
            .AddColumn("[bold]Seção[/]")
            .AddColumn("[bold]Propriedade[/]")
            .AddColumn("[bold]Valor[/]");

        // Paths
        configTable.AddRow("Paths", "DataDir", config.Paths.DataDir);
        configTable.AddRow("", "ParquetDir", config.Paths.ParquetDir);
        configTable.AddRow("", "OutputDir", config.Paths.OutputDir);
        configTable.AddRow("", "DownloadDir", config.Paths.DownloadDir);
        configTable.AddRow("", "HashCacheDir", config.Paths.HashCacheDir);

        // Storage
        configTable.AddRow("Storage", "Type", config.Storage.Type);
        configTable.AddRow("", "Enabled", config.Storage.Enabled.ToString());

        // DuckDB
        configTable.AddRow("DuckDB", "UseInMemory", config.DuckDb.UseInMemory.ToString());
        configTable.AddRow("", "MemoryLimit", config.DuckDb.MemoryLimit);
        configTable.AddRow("", "ThreadsPragma", config.DuckDb.ThreadsPragma.ToString());

        // Processing
        configTable.AddRow("Processing", "BatchUploadSize", config.Ndjson.BatchUploadSize.ToString());
        configTable.AddRow("", "ParallelDownloads", config.Downloader.ParallelDownloads.ToString());

        AnsiConsole.Write(configTable);
        AnsiConsole.WriteLine();

        // Dicas para configuração
        var panel = new Panel(
            "[bold yellow]💡 Dicas de Configuração:[/]\n\n" +
            "• Configure OPENCNPJ_BASE_PATH para definir o diretório raiz de todos os dados\n" +
            "• Use variáveis de ambiente específicas para paths individuais se necessário\n" +
            "• Configure STORAGE_TYPE=filesystem para armazenamento local\n" +
            "• Configure DUCKDB_IN_MEMORY=false para usar armazenamento em disco\n" +
            "• Ajuste DOWNLOADER_PARALLEL para controlar downloads simultâneos\n" +
            "• Use arquivo .env para configurações persistentes"
        )
        {
            Header = new PanelHeader("[bold blue]Ajuda[/]"),
            Border = BoxBorder.Rounded
        };

        AnsiConsole.Write(panel);

        return 0;
    }
}