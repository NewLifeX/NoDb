# NewLife.NoDb - Base de datos NoSQL embebida sobre archivos mapeados en memoria

![GitHub License](https://img.shields.io/github/license/NewLifeX/NewLife.NoDb?logo=github)
![NuGet Downloads](https://img.shields.io/nuget/dt/NewLife.NoDb?logo=nuget)
![NuGet](https://img.shields.io/nuget/v/NewLife.NoDb?logo=nuget)
![NuGet prerelease](https://img.shields.io/nuget/vpre/NewLife.NoDb?label=dev%20nuget&logo=nuget)
[![test](https://github.com/NewLifeX/NewLife.NoDb/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/NewLifeX/NewLife.NoDb/actions/workflows/test.yml)
![platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-blue)
[![contributors](https://img.shields.io/github/contributors/NewLifeX/NewLife.NoDb?logo=github)](https://github.com/NewLifeX/NewLife.NoDb/graphs/contributors)

<!-- lang:start -->
🌐 [中文](Readme.MD) | [繁體中文](README.zh-TW.md) | [English](README.en.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

> Esta traducción puede estar desactualizada; consulte la versión en chino.

NewLife.NoDb es un almacén NoSQL embebido sobre archivos mapeados en memoria, con KV de longitud variable, listas ordenadas, millones de operaciones y persistencia.

## Instalación

```bash
dotnet add package NewLife.NoDb
```

## Inicio rápido

```csharp
var db = new Database("data");
var kv = db.GetKv<String>("users");
kv.Set("a", "hello");
```

## Características

- Alto rendimiento y baja latencia, probado a escala de producción
- Capacidades y datos integrados; funciona sin servicios externos
- Implementación totalmente administrada sin dependencias nativas

## Documentación

> 📖 Documentación completa (en chino): [documentos del repositorio](Doc)

## Licencia

MIT License © 2002-2026 Equipo NewLife

[GitHub](https://github.com/NewLifeX/NewLife.NoDb) | [Website](https://newlifex.com)
