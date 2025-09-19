using CNPJExporter.Configuration;
using Spectre.Console;

namespace CNPJExporter.Exporters;

public class S3Exporter : IStorageExporter
{
    public string Name => "S3";

    public async Task<bool> UploadFolderAsync(string localFolderPath, ProgressTask? progressTask = null)
    {
        AnsiConsole.MarkupLine("[yellow]⚠️ S3 Exporter ainda não implementado. Use FileSystem ou Rclone.[/]");
        await Task.Delay(100); // Placeholder
        return false;
    }

    public async Task<bool> UploadFileAsync(string localFilePath, string remoteRelativePath)
    {
        AnsiConsole.MarkupLine("[yellow]⚠️ S3 Exporter ainda não implementado. Use FileSystem ou Rclone.[/]");
        await Task.Delay(100); // Placeholder
        return false;
    }

    public async Task<bool> DownloadFileAsync(string remoteRelativePath, string localFilePath)
    {
        AnsiConsole.MarkupLine("[yellow]⚠️ S3 Exporter ainda não implementado. Use FileSystem ou Rclone.[/]");
        await Task.Delay(100); // Placeholder
        return false;
    }

    public async Task<bool> IsAvailableAsync()
    {
        // Por enquanto sempre retorna false até implementarmos
        await Task.Delay(1);
        return false;
    }
}