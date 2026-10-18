# NewLife.NoDb - Eingebettete NoSQL-Datenbank auf speichergemappten Dateien

![GitHub License](https://img.shields.io/github/license/NewLifeX/NewLife.NoDb?logo=github)
![NuGet Downloads](https://img.shields.io/nuget/dt/NewLife.NoDb?logo=nuget)
![NuGet](https://img.shields.io/nuget/v/NewLife.NoDb?logo=nuget)
![NuGet prerelease](https://img.shields.io/nuget/vpre/NewLife.NoDb?label=dev%20nuget&logo=nuget)
[![test](https://github.com/NewLifeX/NewLife.NoDb/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/NewLifeX/NewLife.NoDb/actions/workflows/test.yml)
![platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-blue)
[![contributors](https://img.shields.io/github/contributors/NewLifeX/NewLife.NoDb?logo=github)](https://github.com/NewLifeX/NewLife.NoDb/graphs/contributors)

<!-- lang:start -->
🌐 [中文](Readme.MD) | [繁體中文](README.zh-TW.md) | [English](README.en.md) | [Español](README.es.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

> Diese Übersetzung kann hinter der chinesischen Version zurückliegen.

NewLife.NoDb ist ein eingebetteter NoSQL-Speicher auf speichergemappten Dateien mit variabel langen KV-Paaren, geordneten Listen, Millionenoperationen und Persistenz.

## Installation

```bash
dotnet add package NewLife.NoDb
```

## Schnellstart

```csharp
var db = new Database("data");
var kv = db.GetKv<String>("users");
kv.Set("a", "hello");
```

## Funktionen

- Hoher Durchsatz und niedrige Latenz, im Produktivbetrieb erprobt
- Integrierte Funktionen und Daten; läuft ohne externe Dienste
- Rein verwaltete Implementierung ohne native Abhängigkeiten

## Dokumentation

> 📖 Vollständige Dokumentation (Chinesisch): [Repository-Dokumente](Doc)

## Lizenz

MIT License © 2002-2026 NewLife-Team

[GitHub](https://github.com/NewLifeX/NewLife.NoDb) | [Website](https://newlifex.com)
