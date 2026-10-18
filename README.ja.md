# NewLife.NoDb - メモリマップドファイル上の組み込み NoSQL データベース

![GitHub License](https://img.shields.io/github/license/NewLifeX/NewLife.NoDb?logo=github)
![NuGet Downloads](https://img.shields.io/nuget/dt/NewLife.NoDb?logo=nuget)
![NuGet](https://img.shields.io/nuget/v/NewLife.NoDb?logo=nuget)
![NuGet prerelease](https://img.shields.io/nuget/vpre/NewLife.NoDb?label=dev%20nuget&logo=nuget)
[![test](https://github.com/NewLifeX/NewLife.NoDb/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/NewLifeX/NewLife.NoDb/actions/workflows/test.yml)
![platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-blue)
[![contributors](https://img.shields.io/github/contributors/NewLifeX/NewLife.NoDb?logo=github)](https://github.com/NewLifeX/NewLife.NoDb/graphs/contributors)

<!-- lang:start -->
🌐 [中文](Readme.MD) | [繁體中文](README.zh-TW.md) | [English](README.en.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

> この翻訳は中国語版より遅れている場合があります。

NewLife.NoDb はメモリマップドファイル上の組み込み NoSQL ストアで、可変長 KV と順序リストを提供し、数百万回/秒の処理と永続化を実現します。

## インストール

```bash
dotnet add package NewLife.NoDb
```

## クイックスタート

```csharp
var db = new Database("data");
var kv = db.GetKv<String>("users");
kv.Set("a", "hello");
```

## 主な機能

- 高スループット・低遅延で、大規模本番環境で実証済み
- 機能とデータを内蔵し、外部サービスなしで動作
- 純マネージド実装でネイティブ依存なし

## ドキュメント

> 📖 詳細ドキュメント（中国語）： [リポジトリ内ドキュメント](Doc)

## ライセンス

MIT License © 2002-2026 NewLife チーム

[GitHub](https://github.com/NewLifeX/NewLife.NoDb) | [Website](https://newlifex.com)
