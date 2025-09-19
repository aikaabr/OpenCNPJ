using CNPJExporter.Configuration;
using Spectre.Console;

namespace CNPJExporter.Exporters;

public class FileSystemExporter : IStorageExporter
{
    private readonly string _outputPath;

    public string Name => "FileSystem";

    public FileSystemExporter()
    {
        _outputPath = AppConfig.Current.Storage.FileSystem.OutputPath ?? "./output";
    }

    public async Task<bool> UploadFolderAsync(string localFolderPath, ProgressTask? progressTask = null)
    {
        try
        {
            Directory.CreateDirectory(_outputPath);

            var sourceDir = new DirectoryInfo(localFolderPath);
            var files = sourceDir.GetFiles("*", SearchOption.AllDirectories);

            if (progressTask != null)
            {
                progressTask.StartTask();
                progressTask.MaxValue = files.Length;
            }

            foreach (var file in files)
            {
                var relativePath = Path.GetRelativePath(localFolderPath, file.FullName);
                var targetPath = Path.Combine(_outputPath, relativePath);

                var targetDir = Path.GetDirectoryName(targetPath);
                if (!string.IsNullOrEmpty(targetDir))
                {
                    Directory.CreateDirectory(targetDir);
                }

                File.Copy(file.FullName, targetPath, overwrite: true);

                if (progressTask != null)
                {
                    progressTask.Increment(1);
                    progressTask.Description = $"[cyan]Copiando: {relativePath}[/]";
                }

                // Simula algum tempo de processamento para dar feedback visual
                await Task.Delay(1);
            }

            AnsiConsole.MarkupLine($"[green]✓ {files.Length} arquivos copiados para {_outputPath}[/]");
            return true;
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Erro no FileSystem export: {ex.Message.EscapeMarkup()}[/]");
            return false;
        }
    }

    public Task<bool> UploadFileAsync(string localFilePath, string remoteRelativePath)
    {
        try
        {
            var targetPath = Path.Combine(_outputPath, remoteRelativePath.TrimStart('/'));
            var targetDir = Path.GetDirectoryName(targetPath);

            if (!string.IsNullOrEmpty(targetDir))
            {
                Directory.CreateDirectory(targetDir);
            }

            File.Copy(localFilePath, targetPath, overwrite: true);

            AnsiConsole.MarkupLine($"[green]✓ Arquivo copiado: {remoteRelativePath}[/]");
            return Task.FromResult(true);
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Erro ao copiar arquivo: {ex.Message.EscapeMarkup()}[/]");
            return Task.FromResult(false);
        }
    }

    public Task<bool> DownloadFileAsync(string remoteRelativePath, string localFilePath)
    {
        try
        {
            var sourcePath = Path.Combine(_outputPath, remoteRelativePath.TrimStart('/'));

            if (!File.Exists(sourcePath))
            {
                AnsiConsole.MarkupLine($"[yellow]⚠️ Arquivo não encontrado: {sourcePath}[/]");
                return Task.FromResult(false);
            }

            var localDir = Path.GetDirectoryName(localFilePath);
            if (!string.IsNullOrEmpty(localDir))
            {
                Directory.CreateDirectory(localDir);
            }

            File.Copy(sourcePath, localFilePath, overwrite: true);

            AnsiConsole.MarkupLine($"[green]✓ Arquivo baixado: {remoteRelativePath}[/]");
            return Task.FromResult(true);
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Erro ao baixar arquivo: {ex.Message.EscapeMarkup()}[/]");
            return Task.FromResult(false);
        }
    }

    public async Task<bool> IsAvailableAsync()
    {
        try
        {
            Directory.CreateDirectory(_outputPath);

            // Testa escrita criando um arquivo temporário
            var testFile = Path.Combine(_outputPath, ".test_write");
            await File.WriteAllTextAsync(testFile, "test");
            File.Delete(testFile);

            return true;
        }
        catch
        {
            return false;
        }
    }
}