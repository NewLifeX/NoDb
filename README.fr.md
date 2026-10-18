# NewLife.NoDb - Base NoSQL embarquée sur fichiers mappés en mémoire

![GitHub License](https://img.shields.io/github/license/NewLifeX/NewLife.NoDb?logo=github)
![NuGet Downloads](https://img.shields.io/nuget/dt/NewLife.NoDb?logo=nuget)
![NuGet](https://img.shields.io/nuget/v/NewLife.NoDb?logo=nuget)
![NuGet prerelease](https://img.shields.io/nuget/vpre/NewLife.NoDb?label=dev%20nuget&logo=nuget)
[![test](https://github.com/NewLifeX/NewLife.NoDb/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/NewLifeX/NewLife.NoDb/actions/workflows/test.yml)
![platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-blue)
[![contributors](https://img.shields.io/github/contributors/NewLifeX/NewLife.NoDb?logo=github)](https://github.com/NewLifeX/NewLife.NoDb/graphs/contributors)

<!-- lang:start -->
🌐 [中文](Readme.MD) | [繁體中文](README.zh-TW.md) | [English](README.en.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

> Cette traduction peut être en retard sur la version chinoise.

NewLife.NoDb est un stockage NoSQL embarqué sur fichiers mappés en mémoire : KV à longueur variable, listes ordonnées, millions d'opérations et persistance.

## Installation

```bash
dotnet add package NewLife.NoDb
```

## Démarrage rapide

```csharp
var db = new Database("data");
var kv = db.GetKv<String>("users");
kv.Set("a", "hello");
```

## Fonctionnalités

- Haut débit et faible latence, éprouvés à l'échelle de la production
- Fonctionnalités et données intégrées ; fonctionne sans service externe
- Implémentation entièrement managée sans dépendances natives

## Documentation

> 📖 Documentation complète (chinois) : [documents du dépôt](Doc)

## Licence

MIT License © 2002-2026 Équipe NewLife

[GitHub](https://github.com/NewLifeX/NewLife.NoDb) | [Website](https://newlifex.com)
