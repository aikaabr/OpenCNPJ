﻿using CNPJExporter.Commands;
using CNPJExporter.Configuration;
using Spectre.Console;
using Spectre.Console.Cli;

// Carregar variáveis de ambiente do arquivo .env
DotEnvLoader.Load();

// Carregar configuração (agora com suporte a variáveis de ambiente)
AppConfig.Load();

AnsiConsole.MarkupLine("[bold blue]🚀 OpenCNPJ ETL Processor[/]");

var app = new CommandApp();
app.Configure(config =>
{
    config.SetApplicationName("opencnpj-etl");
    config.ValidateExamples();

    config.AddCommand<SingleCommand>("single").WithDescription("Processa CNPJ específico");
    config.AddCommand<TestCommand>("test").WithDescription("Testa integridade de dados");
    config.AddCommand<ZipCommand>("zip").WithDescription("Gera ZIP consolidado local");
    config.AddCommand<PipelineCommand>("pipeline").WithDescription("Pipeline completo (download → ingest → upload → test → zip)");
    config.AddCommand<ConfigCommand>("config").WithDescription("Exibe e valida configurações do sistema");
});

return app.Run(args);
