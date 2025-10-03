#!/usr/bin/env python3
"""
Redis Sync工具的安装配置文件
"""

from setuptools import setup, find_packages
import os

# 读取README文件
def read_readme():
    readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return "Redis Sync - 全面的Redis迁移和同步工具"

# 读取requirements文件
def read_requirements():
    requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    if os.path.exists(requirements_path):
        with open(requirements_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return [
        'redis>=4.0.0',
        'click>=8.0.0',
        'pyyaml>=6.0',
        'tqdm>=4.60.0'
    ]

setup(
    name="redis-sync",
    version="1.0.0",
    author="redisSync Contributors",
    author_email="",
    description="High-performance Redis synchronization service with one-to-many support, optimized for cross-border transmission",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/redisSync",
    project_urls={
        "Bug Tracker": "https://github.com/yourusername/redisSync/issues",
        "Documentation": "https://github.com/yourusername/redisSync#readme",
        "Source Code": "https://github.com/yourusername/redisSync",
        "Changelog": "https://github.com/yourusername/redisSync/blob/main/CHANGELOG.md",
    },
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: System :: Systems Administration",
        "Topic :: System :: Networking",
        "Topic :: Utilities",
        "Environment :: Console",
        "Environment :: Web Environment",
    ],
    python_requires=">=3.7",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=6.0.0",
            "pytest-cov>=2.10.0",
            "black>=21.0.0",
            "flake8>=3.8.0",
            "mypy>=0.800",
        ],
        "docs": [
            "sphinx>=4.0.0",
            "sphinx-rtd-theme>=0.5.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "redis-sync=redis_sync.cli:main",
        ],
    },
    include_package_data=True,
    package_data={
        "redis_sync": [
            "*.yaml",
            "*.yml",
            "*.json",
        ],
    },
    keywords=[
        "redis",
        "sync",
        "synchronization",
        "replication",
        "migration",
        "database",
        "data-transfer",
        "backup",
        "restore",
        "cross-border",
        "one-to-many",
        "incremental-sync",
        "real-time",
    ],
    zip_safe=False,
)
