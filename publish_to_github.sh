#!/bin/bash

# Redis Sync 项目发布到 GitHub 的脚本
# 作者：tigerainiuniu
# 日期：2025-01-03

set -e  # 遇到错误立即退出

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                                                              ║"
echo "║   🚀 Redis Sync 项目发布到 GitHub                            ║"
echo "║                                                              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 项目信息
GITHUB_USER="tigerainiuniu"
REPO_NAME="redisSync"
REPO_URL="https://github.com/${GITHUB_USER}/${REPO_NAME}.git"

# 步骤 1：检查敏感信息
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📝 步骤 1/6: 检查敏感信息"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# 检查配置文件
if [ -f "config.yaml" ]; then
    echo -e "${YELLOW}⚠️  警告：发现 config.yaml 文件${NC}"
    echo "   这个文件包含敏感信息，将被 .gitignore 忽略"
fi

if [ -f "config_prod.yaml" ]; then
    echo -e "${YELLOW}⚠️  警告：发现 config_prod.yaml 文件${NC}"
    echo "   这个文件包含敏感信息，将被 .gitignore 忽略"
fi

# 检查日志文件
if ls *.log 1> /dev/null 2>&1; then
    echo -e "${YELLOW}⚠️  警告：发现日志文件${NC}"
    echo "   日志文件将被 .gitignore 忽略"
fi

echo -e "${GREEN}✅ 敏感信息检查完成${NC}"
echo ""

# 步骤 2：初始化 Git 仓库
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📦 步骤 2/6: 初始化 Git 仓库"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -d ".git" ]; then
    echo -e "${YELLOW}⚠️  Git 仓库已存在，跳过初始化${NC}"
else
    git init
    echo -e "${GREEN}✅ Git 仓库初始化完成${NC}"
fi
echo ""

# 步骤 3：添加文件
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📁 步骤 3/6: 添加文件到 Git"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

git add .

echo "将要提交的文件："
git status --short

echo ""
echo -e "${YELLOW}⚠️  请确认以下文件不会被提交：${NC}"
echo "   - config.yaml"
echo "   - config_prod.yaml"
echo "   - *.log"
echo ""

# 验证 .gitignore 是否生效
if git check-ignore -q config.yaml; then
    echo -e "${GREEN}✅ config.yaml 已被忽略${NC}"
else
    echo -e "${RED}❌ 警告：config.yaml 未被忽略！${NC}"
fi

if git check-ignore -q config_prod.yaml; then
    echo -e "${GREEN}✅ config_prod.yaml 已被忽略${NC}"
else
    echo -e "${RED}❌ 警告：config_prod.yaml 未被忽略！${NC}"
fi

echo ""
read -p "是否继续？(y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "已取消"
    exit 1
fi

# 步骤 4：提交
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "💾 步骤 4/6: 提交到本地仓库"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

git commit -m "feat: 初始版本 v1.0.0 - Redis 一对多同步服务

✨ 主要特性：
- 一对多同步：支持一个源 Redis 同步到多个目标
- 性能优化：统一扫描 + 并行分发，性能提升 67%
- 三种同步模式：SCAN、SYNC、PSYNC
- REPLCONF ACK 心跳机制：完整实现 Redis 复制协议
- 跨境优化：自动重连、重试机制
- Web 管理界面：实时监控同步状态

📊 支持的数据类型：
- String、Hash、List、Set、ZSet、Stream

🔧 核心功能：
- 全量 + 增量同步
- TTL 保持
- 命令去重
- 故障恢复
- 详细日志
"

echo -e "${GREEN}✅ 提交完成${NC}"
echo ""

# 步骤 5：添加远程仓库
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔗 步骤 5/6: 添加远程仓库"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# 检查远程仓库是否已存在
if git remote | grep -q "origin"; then
    echo -e "${YELLOW}⚠️  远程仓库 origin 已存在${NC}"
    CURRENT_URL=$(git remote get-url origin)
    echo "   当前 URL: ${CURRENT_URL}"
    
    if [ "$CURRENT_URL" != "$REPO_URL" ]; then
        echo "   更新为: ${REPO_URL}"
        git remote set-url origin "$REPO_URL"
    fi
else
    git remote add origin "$REPO_URL"
    echo -e "${GREEN}✅ 远程仓库添加完成${NC}"
fi

echo ""
echo "远程仓库信息："
git remote -v
echo ""

# 步骤 6：推送到 GitHub
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🚀 步骤 6/6: 推送到 GitHub"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo -e "${YELLOW}⚠️  请确保你已经在 GitHub 上创建了仓库：${NC}"
echo "   https://github.com/${GITHUB_USER}/${REPO_NAME}"
echo ""
echo "如果还没有创建，请："
echo "1. 访问 https://github.com/new"
echo "2. 仓库名称：${REPO_NAME}"
echo "3. 描述：高性能 Redis 一对多同步服务，支持跨境传输优化"
echo "4. 可见性：Public（公开）"
echo "5. 不要勾选任何初始化选项（README、.gitignore、License）"
echo "6. 点击 Create repository"
echo ""

read -p "已经创建了 GitHub 仓库？(y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "请先创建 GitHub 仓库，然后重新运行此脚本"
    exit 1
fi

# 推送代码
git branch -M main
git push -u origin main

echo ""
echo -e "${GREEN}✅ 推送完成！${NC}"
echo ""

# 完成
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                                                              ║"
echo "║   🎉 恭喜！项目已成功发布到 GitHub！                         ║"
echo "║                                                              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "📍 项目地址："
echo "   https://github.com/${GITHUB_USER}/${REPO_NAME}"
echo ""
echo "📝 下一步："
echo "   1. 访问项目页面，检查文件是否正确"
echo "   2. 创建第一个 Release (v1.0.0)"
echo "   3. 添加 Topics 标签（redis, sync, migration, python）"
echo "   4. 编写项目介绍和使用文档"
echo ""
echo "🎊 祝你的开源项目成功！"
echo ""

