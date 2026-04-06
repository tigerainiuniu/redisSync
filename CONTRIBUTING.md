# 贡献指南 / Contributing Guide

感谢你对 redisSync 项目的关注！我们欢迎任何形式的贡献。

Thank you for your interest in contributing to redisSync! We welcome all forms of contributions.

## 🌟 如何贡献 / How to Contribute

### 报告问题 / Reporting Issues

如果你发现了 bug 或有功能建议，请：

1. 检查 [Issues](https://github.com/tigerainiuniu/redisSync/issues) 确保问题未被报告
2. 创建新的 Issue，提供详细信息：
   - 问题描述
   - 复现步骤
   - 预期行为
   - 实际行为
   - 环境信息（Python 版本、Redis 版本等）

### 提交代码 / Submitting Code

1. **Fork 项目**
   ```bash
   # Fork 项目到你的 GitHub 账号
   # 然后克隆你的 fork
   git clone https://github.com/YOUR_USERNAME/redisSync.git
   cd redisSync
   ```

2. **创建分支**
   ```bash
   git checkout -b feature/your-feature-name
   # 或
   git checkout -b fix/your-bug-fix
   ```

3. **开发和测试**
   ```bash
   # 安装开发依赖
   pip install -r requirements.txt
   
   # 进行你的修改
   # 确保代码符合项目规范
   
   # 测试你的修改
   python -m pytest  # 如果有测试
   ```

4. **提交更改**
   ```bash
   git add .
   git commit -m "feat: 添加新功能描述"
   # 或
   git commit -m "fix: 修复问题描述"
   ```

5. **推送并创建 Pull Request**
   ```bash
   git push origin feature/your-feature-name
   ```
   
   然后在 GitHub 上创建 Pull Request。

## 📝 代码规范 / Code Style

### Python 代码规范

- 遵循 [PEP 8](https://www.python.org/dev/peps/pep-0008/) 规范
- 使用 4 个空格缩进
- 函数和方法添加文档字符串
- 变量和函数使用有意义的名称

### 提交信息规范

使用语义化的提交信息：

- `feat:` 新功能
- `fix:` 修复 bug
- `docs:` 文档更新
- `style:` 代码格式调整
- `refactor:` 代码重构
- `test:` 测试相关
- `chore:` 构建/工具相关

示例：
```
feat: 添加 REPLCONF ACK 心跳机制
fix: 修复 PSYNC 连接断开问题
docs: 更新 README 安装说明
```

## 🧪 测试 / Testing

在提交 PR 之前，请确保：

1. 代码能正常运行
2. 没有引入新的 bug
3. 如果可能，添加测试用例

## 📚 文档 / Documentation

如果你的更改涉及：

- 新功能：更新 README.md
- API 变更：更新相关文档
- 配置变更：更新 config.yaml.example

## 🤝 行为准则 / Code of Conduct

- 尊重所有贡献者
- 保持友好和专业
- 接受建设性的批评
- 关注对项目最有利的事情

## 💡 开发建议 / Development Tips

### 项目结构

```
redisSync/
├── redis_sync/              # 核心代码
│   ├── connection_manager.py
│   ├── full_migration_handler.py
│   ├── incremental_migration_handler.py
│   ├── psync_incremental_handler.py
│   ├── scan_handler.py
│   └── ...
├── config.yaml.example      # 配置示例
├── run_sync_service.py      # 主入口
└── README.md
```

### 关键模块

- **connection_manager.py**: Redis 连接管理
- **full_migration_handler.py**: 全量迁移
- **incremental_migration_handler.py**: 增量同步
- **psync_incremental_handler.py**: PSYNC 协议实现
- **scan_handler.py**: SCAN 模式实现

### 调试技巧

1. **启用详细日志**
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

2. **测试单个模块**
   ```bash
   python -c "from redis_sync.scan_handler import ScanHandler; ..."
   ```

3. **使用本地 Redis 测试**
   ```bash
   # 启动两个 Redis 实例
   redis-server --port 6379
   redis-server --port 6380
   ```

## 🎯 优先级 / Priority

我们特别欢迎以下方面的贡献：

- 🐛 Bug 修复
- 📝 文档改进
- ✨ 性能优化
- 🧪 测试用例
- 🌍 国际化支持
- 🔧 新功能（请先创建 Issue 讨论）

## 📞 联系方式 / Contact

- GitHub Issues: [提交 Issue](https://github.com/tigerainiuniu/redisSync/issues)
- Pull Requests: [提交 PR](https://github.com/tigerainiuniu/redisSync/pulls)

## 🙏 致谢 / Acknowledgments

感谢所有为 redisSync 做出贡献的开发者！

---

再次感谢你的贡献！ / Thank you again for your contribution!

