# NewLife.NoDb - Banco NoSQL embarcado em arquivos mapeados em memória

![GitHub License](https://img.shields.io/github/license/NewLifeX/NewLife.NoDb?logo=github)
![NuGet Downloads](https://img.shields.io/nuget/dt/NewLife.NoDb?logo=nuget)
![NuGet](https://img.shields.io/nuget/v/NewLife.NoDb?logo=nuget)
![NuGet prerelease](https://img.shields.io/nuget/vpre/NewLife.NoDb?label=dev%20nuget&logo=nuget)
[![test](https://github.com/NewLifeX/NewLife.NoDb/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/NewLifeX/NewLife.NoDb/actions/workflows/test.yml)
![platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-blue)
![arch](https://img.shields.io/badge/arch-x64%20%7C%20arm64%20%7C%20loongarch64-orange)
[![contributors](https://img.shields.io/github/contributors/NewLifeX/NewLife.NoDb?logo=github)](https://github.com/NewLifeX/NewLife.NoDb/graphs/contributors)

<!-- lang:start -->
🌐 [中文](Readme.MD) | [繁體中文](README.zh-TW.md) | [English](README.en.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

> Esta tradução pode estar desatualizada em relação à versão em chinês.

NewLife.NoDb é um armazenamento NoSQL embarcado em arquivos mapeados em memória, com KV de tamanho variável, listas ordenadas, milhões de operações e persistência.

## Instalação

```bash
dotnet add package NewLife.NoDb
```

## Início rápido

```csharp
var db = new Database("data");
var kv = db.GetKv<String>("users");
kv.Set("a", "hello");
```

## Recursos

- Alta taxa de transferência e baixa latência, comprovados em produção
- Recursos e dados integrados; funciona sem serviços externos
- Implementação totalmente gerenciada sem dependências nativas

## Documentação

> 📖 Documentação completa (em chinês): [documentos do repositório](Doc)

## Licença

MIT License © 2002-2026 Equipe NewLife

[GitHub](https://github.com/NewLifeX/NewLife.NoDb) | [Website](https://newlifex.com)
