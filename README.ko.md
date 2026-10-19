# NewLife.NoDb - 메모리 매핑 파일 기반 임베디드 NoSQL 데이터베이스

![GitHub License](https://img.shields.io/github/license/NewLifeX/NewLife.NoDb?logo=github)
![NuGet Downloads](https://img.shields.io/nuget/dt/NewLife.NoDb?logo=nuget)
![NuGet](https://img.shields.io/nuget/v/NewLife.NoDb?logo=nuget)
![NuGet prerelease](https://img.shields.io/nuget/vpre/NewLife.NoDb?label=dev%20nuget&logo=nuget)
[![test](https://github.com/NewLifeX/NewLife.NoDb/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/NewLifeX/NewLife.NoDb/actions/workflows/test.yml)
![platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-blue)
![arch](https://img.shields.io/badge/arch-x64%20%7C%20arm64%20%7C%20loongarch64-orange)
[![contributors](https://img.shields.io/github/contributors/NewLifeX/NewLife.NoDb?logo=github)](https://github.com/NewLifeX/NewLife.NoDb/graphs/contributors)

<!-- lang:start -->
🌐 [中文](Readme.MD) | [繁體中文](README.zh-TW.md) | [English](README.en.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [العربية](README.ar.md)
<!-- lang:end -->

> 이 번역은 중국어 버전보다 늦을 수 있습니다.

NewLife.NoDb는 메모리 매핑 파일 기반 임베디드 NoSQL 저장소로, 가변 길이 KV와 순차 목록을 제공하며 수백만 회 처리와 영속성을 지원합니다.

## 설치

```bash
dotnet add package NewLife.NoDb
```

## 빠른 시작

```csharp
var db = new Database("data");
var kv = db.GetKv<String>("users");
kv.Set("a", "hello");
```

## 주요 기능

- 높은 처리량과 낮은 지연, 대규모 운영 환경에서 검증됨
- 기능과 데이터 내장, 외부 서비스 없이 동작
- 네이티브 의존성 없는 순수 관리형 구현

## 문서

> 📖 전체 문서(중국어): [저장소 문서](Doc)

## 라이선스

MIT License © 2002-2026 NewLife 팀

[GitHub](https://github.com/NewLifeX/NewLife.NoDb) | [Website](https://newlifex.com)
