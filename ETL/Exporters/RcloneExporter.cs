using System.Diagnostics;
using CNPJExporter.Configuration;
using Spectre.Console;

namespace CNPJExporter.Exporters;

public class RcloneExporter : IStorageExporter
{
    public string Name => "Rclone";

    public async Task<bool> UploadFolderAsync(string localFolderPath, ProgressTask? progressTask = null)
    {
        return await RcloneClient.UploadFolderAsync(localFolderPath, progressTask);
    }

    public async Task<bool> UploadFileAsync(string localFilePath, string remoteRelativePath)
    {
        return await RcloneClient.UploadFileAsync(localFilePath, remoteRelativePath);
    }

    public async Task<bool> DownloadFileAsync(string remoteRelativePath, string localFilePath)
    {
        return await RcloneClient.DownloadFileAsync(remoteRelativePath, localFilePath);
    }

    public async Task<bool> IsAvailableAsync()
    {
        try
        {
            using var process = new Process();
            process.StartInfo.FileName = "rclone";
            process.StartInfo.Arguments = "version";
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.StartInfo.CreateNoWindow = true;

            process.Start();
            await process.WaitForExitAsync();

            return process.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }
}