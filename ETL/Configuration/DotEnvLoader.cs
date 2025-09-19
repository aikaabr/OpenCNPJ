using System;
using System.IO;

namespace CNPJExporter.Configuration;

/// <summary>
/// Utilitário para carregar variáveis de ambiente de um arquivo .env
/// </summary>
public static class DotEnvLoader
{
    /// <summary>
    /// Carrega variáveis de ambiente do arquivo .env no diretório atual
    /// </summary>
    public static void Load()
    {
        Load(Path.Combine(Environment.CurrentDirectory, ".env"));
    }

    /// <summary>
    /// Carrega variáveis de ambiente de um arquivo específico
    /// </summary>
    /// <param name="filePath">Caminho para o arquivo .env</param>
    public static void Load(string filePath)
    {
        if (!File.Exists(filePath))
        {
            return;
        }

        try
        {
            var lines = File.ReadAllLines(filePath);

            foreach (var line in lines)
            {
                var trimmedLine = line.Trim();

                // Pular comentários e linhas vazias
                if (string.IsNullOrEmpty(trimmedLine) || trimmedLine.StartsWith("#"))
                {
                    continue;
                }

                // Procurar por '=' para separar chave e valor
                var equalsIndex = trimmedLine.IndexOf('=');
                if (equalsIndex <= 0)
                {
                    continue;
                }

                var key = trimmedLine.Substring(0, equalsIndex).Trim();
                var value = trimmedLine.Substring(equalsIndex + 1).Trim();

                // Remover aspas se existirem
                if ((value.StartsWith("\"") && value.EndsWith("\"")) ||
                    (value.StartsWith("'") && value.EndsWith("'")))
                {
                    value = value.Substring(1, value.Length - 2);
                }

                // Remover comentários inline
                var commentIndex = value.IndexOf("#");
                if (commentIndex >= 0)
                {
                    value = value.Substring(0, commentIndex).Trim();
                }

                // Só definir se a variável não existir no ambiente
                if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(key)))
                {
                    Environment.SetEnvironmentVariable(key, value);
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erro ao carregar arquivo .env '{filePath}': {ex.Message}");
        }
    }

    /// <summary>
    /// Carrega variáveis de ambiente e retorna quantas foram carregadas
    /// </summary>
    /// <param name="filePath">Caminho para o arquivo .env</param>
    /// <returns>Número de variáveis carregadas</returns>
    public static int LoadWithCount(string? filePath = null)
    {
        filePath ??= Path.Combine(Environment.CurrentDirectory, ".env");

        if (!File.Exists(filePath))
        {
            return 0;
        }

        var count = 0;

        try
        {
            var lines = File.ReadAllLines(filePath);

            foreach (var line in lines)
            {
                var trimmedLine = line.Trim();

                if (string.IsNullOrEmpty(trimmedLine) || trimmedLine.StartsWith("#"))
                {
                    continue;
                }

                var equalsIndex = trimmedLine.IndexOf('=');
                if (equalsIndex <= 0)
                {
                    continue;
                }

                var key = trimmedLine.Substring(0, equalsIndex).Trim();
                var value = trimmedLine.Substring(equalsIndex + 1).Trim();

                if ((value.StartsWith("\"") && value.EndsWith("\"")) ||
                    (value.StartsWith("'") && value.EndsWith("'")))
                {
                    value = value.Substring(1, value.Length - 2);
                }

                var commentIndex = value.IndexOf("#");
                if (commentIndex >= 0)
                {
                    value = value.Substring(0, commentIndex).Trim();
                }

                if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(key)))
                {
                    Environment.SetEnvironmentVariable(key, value);
                    count++;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erro ao carregar arquivo .env '{filePath}': {ex.Message}");
        }

        return count;
    }
}