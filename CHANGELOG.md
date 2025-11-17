# Changelog

All notable changes to Koldun Platform will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.8] - 2025-11-17

### Added
- OpenAI API compatibility audit document (context/OPENAI_COMPATIBILITY_AUDIT.md)
- Response normalization for dllama-api outputs in LLM server
- Comprehensive unit tests for response normalization

### Fixed
- `/v1/models` response now includes required OpenAI fields (`created`, `owned_by`)
- Chat completion responses normalize negative `index` values to valid integers
- Chat completion responses fix empty `finish_reason` to "stop"
- Model name preservation in responses (no longer returns "Distributed Model")
- Improved compatibility with Python openai library and standard OpenAI clients

## [0.0.7] - 2025-11-17

### Added
- ARM64 Rosetta+VZ requirements documentation in README
- CPU inference warnings and NATS queue usage guidelines
- Comprehensive local development instructions

### Fixed
- Auto-recreate NATS consumer on filter subject mismatch
- Improved dllama ARM64 compatibility with optimized build flags

## [0.0.6] - 2025-11-17

### Fixed
- Simplified dllama build to use default flags
- Pinned Alpine version to 3.20 for stable builds
- Busted build cache to fix stale Alpine layers

## [0.0.5] - 2025-11-16

### Added
- Initial ARM64 support with TERMUX_VERSION build flag
- dllama-api integration for distributed inference

### Fixed
- CPU compatibility issues with -march=native flag

## [0.0.4] - 2025-11-15

### Added
- Session lifecycle management via NATS KV
- Ingress backend with OpenAI-compatible API

## [0.0.3] - 2025-11-14

### Added
- Model controller with automatic download and conversion
- Worker controller for distributed topology

## [0.0.2] - 2025-11-13

### Added
- Root controller for dllama-api coordination
- Dllama controller for topology management

## [0.0.1] - 2025-11-12

### Added
- Initial release of Koldun Platform
- Custom Resource Definitions (Session, Dllama, Model, Root, Worker, Ingress)
- Kubernetes operator with Wrangler controllers
- NATS JetStream integration for message routing

---

[0.0.8]: https://github.com/gorizond/koldun/compare/v0.0.7...v0.0.8
[0.0.7]: https://github.com/gorizond/koldun/compare/v0.0.6...v0.0.7
[0.0.6]: https://github.com/gorizond/koldun/compare/v0.0.5...v0.0.6
[0.0.5]: https://github.com/gorizond/koldun/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/gorizond/koldun/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/gorizond/koldun/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/gorizond/koldun/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/gorizond/koldun/releases/tag/v0.0.1
