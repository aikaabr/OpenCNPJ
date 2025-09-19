using Spectre.Console;

namespace CNPJExporter.Exporters;

public interface IStorageExporter
{
    /// <summary>
    /// Faz upload de uma pasta completa para o storage
    /// </summary>
    Task<bool> UploadFolderAsync(string localFolderPath, ProgressTask? progressTask = null);

    /// <summary>
    /// Faz upload de um arquivo específico para o storage
    /// </summary>
    Task<bool> UploadFileAsync(string localFilePath, string remoteRelativePath);

    /// <summary>
    /// Faz download de um arquivo do storage
    /// </summary>
    Task<bool> DownloadFileAsync(string remoteRelativePath, string localFilePath);

    /// <summary>
    /// Verifica se o exporter está disponível e configurado corretamente
    /// </summary>
    Task<bool> IsAvailableAsync();

    /// <summary>
    /// Nome do exporter para logs
    /// </summary>
    string Name { get; }
}