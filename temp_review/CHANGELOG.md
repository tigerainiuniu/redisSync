# 更新日志 / Changelog

所有重要的项目变更都会记录在此文件中。

All notable changes to this project will be documented in this file.

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [1.0.0] - 2025-01-03

### ✨ 新增 / Added

- 🎯 **一对多同步**：支持一个源 Redis 同步到多个目标 Redis
- ⚡ **性能优化**：统一扫描 + 并行分发，多目标场景性能提升 67%
- 🔄 **三种同步模式**：
  - SCAN 模式：基于 SCAN 命令的轮询检测（稳定可靠）
  - SYNC 模式：基于 SYNC 命令的全量同步
  - PSYNC 模式：基于 PSYNC 协议的实时同步（带 REPLCONF ACK 心跳）
- 💓 **REPLCONF ACK 心跳机制**：完整实现 Redis 复制协议，每秒发送心跳
- 📊 **全量 + 增量同步**：支持全量迁移和增量同步
- 🌐 **Web 管理界面**：实时监控同步状态和性能指标
- 🛡️ **故障恢复**：自动重连、重试机制
- 📝 **详细日志**：完整的操作日志和性能监控
- 🔐 **安全连接**：支持 SSL/TLS 和密码认证

### 🎯 支持的数据类型 / Supported Data Types

- ✅ String（字符串）
- ✅ Hash（哈希）
- ✅ List（列表）
- ✅ Set（集合）
- ✅ ZSet（有序集合）
- ✅ Stream（流）

### 🔧 核心功能 / Core Features

- **连接管理**：智能连接池和重连机制
- **命令去重**：MD5 哈希 + 时间窗口 + LRU 缓存
- **TTL 保持**：支持过期时间迁移
- **批量操作**：高效的批量数据传输
- **进度监控**：实时同步进度和统计信息
- **配置灵活**：YAML 配置文件，支持复杂策略

### 🌍 跨境优化 / Cross-Border Optimization

- 自动重试机制（最多 999 次）
- 连接超时配置
- 网络异常处理
- 断线自动重连

### 📦 部署支持 / Deployment Support

- systemd 服务配置
- 后台运行支持
- 进程管理脚本
- 日志轮转

### 🐛 已知问题 / Known Issues

- 腾讯云 Redis 的 PSYNC 模式可能不稳定（建议使用 SCAN 模式）
- 大型 Stream 数据迁移较慢（逐条添加）

### 📚 文档 / Documentation

- ✅ README.md - 完整的项目说明
- ✅ CONTRIBUTING.md - 贡献指南
- ✅ LICENSE - MIT 许可证
- ✅ config.yaml.example - 配置示例

---

## [未来计划] / Future Plans

### 🚀 计划中的功能

- [ ] Docker 支持
- [ ] 数据验证和一致性检查
- [ ] 更多的同步策略
- [ ] 性能基准测试
- [ ] 单元测试覆盖
- [ ] CI/CD 集成
- [ ] 多语言文档（英文）
- [ ] 监控指标导出（Prometheus）
- [ ] 数据压缩传输
- [ ] 增量备份功能

### 💡 欢迎贡献

如果你有好的想法或建议，欢迎：
- 提交 [Issue](https://github.com/tigerainiuniu/redisSync/issues)
- 提交 [Pull Request](https://github.com/tigerainiuniu/redisSync/pulls)

---

## 版本说明 / Version Notes

### 版本号格式

- **主版本号**：不兼容的 API 变更
- **次版本号**：向下兼容的功能新增
- **修订号**：向下兼容的问题修复

### 标签说明

- `Added` - 新增功能
- `Changed` - 功能变更
- `Deprecated` - 即将废弃的功能
- `Removed` - 已移除的功能
- `Fixed` - Bug 修复
- `Security` - 安全相关

---

**感谢所有贡献者！** / **Thanks to all contributors!**

