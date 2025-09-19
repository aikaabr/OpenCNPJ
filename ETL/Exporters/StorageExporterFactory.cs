using CNPJExporter.Configuration;
using Spectre.Console;

namespace CNPJExporter.Exporters;

public static class StorageExporterFactory
{
    private static IStorageExporter? _cachedExporter;

    public static async Task<IStorageExporter?> CreateAsync()
    {
        if (_cachedExporter != null)
            return _cachedExporter;

        if (!AppConfig.Current.Storage.Enabled)
        {
            AnsiConsole.MarkupLine("[yellow]⚠️ Storage export desabilitado na configuração[/]");
            return null;
        }

        var storageType = AppConfig.Current.Storage.Type.ToLowerInvariant();

        IStorageExporter? exporter = storageType switch
        {
            "filesystem" => new FileSystemExporter(),
            "rclone" => new RcloneExporter(),
            "s3" => new S3Exporter(),
            _ => new RcloneExporter() // Default para backward compatibility
        };

        if (exporter != null && await exporter.IsAvailableAsync())
        {
            AnsiConsole.MarkupLine($"[green]✓ Storage exporter '{exporter.Name}' configurado e disponível[/]");
            _cachedExporter = exporter;
            return exporter;
        }

        AnsiConsole.MarkupLine($"[red]❌ Storage exporter '{storageType}' não está disponível[/]");

        // Fallback: tenta outros exporters
        var fallbackExporters = new IStorageExporter[]
        {
            new FileSystemExporter(),
            new RcloneExporter()
        };

        foreach (var fallback in fallbackExporters)
        {
            if (await fallback.IsAvailableAsync())
            {
                AnsiConsole.MarkupLine($"[yellow]⚠️ Usando fallback: {fallback.Name}[/]");
                _cachedExporter = fallback;
                return fallback;
            }
        }

        AnsiConsole.MarkupLine("[red]❌ Nenhum storage exporter disponível[/]");
        return null;
    }

    public static void ClearCache()
    {
        _cachedExporter = null;
    }
}