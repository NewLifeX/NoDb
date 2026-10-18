# NewLife.NoDb - Встроенная NoSQL-база на отображаемых в память файлах

![GitHub License](https://img.shields.io/github/license/NewLifeX/NewLife.NoDb?logo=github)
![NuGet Downloads](https://img.shields.io/nuget/dt/NewLife.NoDb?logo=nuget)
![NuGet](https://img.shields.io/nuget/v/NewLife.NoDb?logo=nuget)
![NuGet prerelease](https://img.shields.io/nuget/vpre/NewLife.NoDb?label=dev%20nuget&logo=nuget)
[![test](https://github.com/NewLifeX/NewLife.NoDb/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/NewLifeX/NewLife.NoDb/actions/workflows/test.yml)
![platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-blue)
[![contributors](https://img.shields.io/github/contributors/NewLifeX/NewLife.NoDb?logo=github)](https://github.com/NewLifeX/NewLife.NoDb/graphs/contributors)

<!-- lang:start -->
🌐 [中文](Readme.MD) | [繁體中文](README.zh-TW.md) | [English](README.en.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

> Перевод может отставать от китайской версии.

NewLife.NoDb — встроенное NoSQL-хранилище на отображаемых в память файлах: KV переменной длины, упорядоченные списки, миллионы операций и персистентность.

## Установка

```bash
dotnet add package NewLife.NoDb
```

## Быстрый старт

```csharp
var db = new Database("data");
var kv = db.GetKv<String>("users");
kv.Set("a", "hello");
```

## Возможности

- Высокая пропускная способность и низкая задержка, проверено в продакшене
- Встроенные возможности и данные; работает без внешних сервисов
- Полностью управляемая реализация без нативных зависимостей

## Документация

> 📖 Полная документация (на китайском): [документы репозитория](Doc)

## Лицензия

MIT License © 2002-2026 Команда NewLife

[GitHub](https://github.com/NewLifeX/NewLife.NoDb) | [Website](https://newlifex.com)
