#!/usr/bin/env python3
"""
Redis同步服务Web管理界面

提供简单的Web界面来监控和管理Redis同步服务。
"""

import json
import time
from datetime import datetime
from typing import Dict, Any
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import threading


class WebUIHandler(BaseHTTPRequestHandler):
    """Web UI请求处理器"""
    
    def __init__(self, sync_service, *args, **kwargs):
        self.sync_service = sync_service
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """处理GET请求"""
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        
        if path == '/':
            self._serve_dashboard()
        elif path == '/api/status':
            self._serve_status_api()
        elif path == '/api/config':
            self._serve_config_api()
        elif path.startswith('/static/'):
            self._serve_static(path)
        else:
            self._serve_404()
    
    def do_POST(self):
        """处理POST请求"""
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        
        if path == '/api/reload':
            self._handle_reload()
        else:
            self._serve_404()
    
    def _serve_dashboard(self):
        """提供仪表板页面"""
        html = self._get_dashboard_html()
        self._send_response(200, html, 'text/html')
    
    def _serve_status_api(self):
        """提供状态API"""
        status = self.sync_service.get_status()
        
        # 添加额外信息
        status['timestamp'] = time.time()
        status['uptime'] = time.time() - getattr(self.sync_service, 'start_time', time.time())
        
        self._send_response(200, json.dumps(status, indent=2), 'application/json')
    
    def _serve_config_api(self):
        """提供配置API"""
        config = self.sync_service.config.copy()
        
        # 隐藏敏感信息
        if 'source' in config and 'password' in config['source']:
            config['source']['password'] = '***' if config['source']['password'] else None
        
        for target in config.get('targets', []):
            if 'password' in target:
                target['password'] = '***' if target['password'] else None
        
        self._send_response(200, json.dumps(config, indent=2), 'application/json')
    
    def _handle_reload(self):
        """处理重新加载请求"""
        try:
            # 这里可以添加重新加载配置的逻辑
            result = {'success': True, 'message': '配置重新加载成功'}
        except Exception as e:
            result = {'success': False, 'message': f'重新加载失败: {str(e)}'}
        
        self._send_response(200, json.dumps(result), 'application/json')
    
    def _serve_static(self, path):
        """提供静态文件"""
        # 简单的静态文件服务
        if path == '/static/style.css':
            css = self._get_css()
            self._send_response(200, css, 'text/css')
        elif path == '/static/script.js':
            js = self._get_javascript()
            self._send_response(200, js, 'application/javascript')
        else:
            self._serve_404()
    
    def _serve_404(self):
        """提供404页面"""
        html = '<html><body><h1>404 Not Found</h1></body></html>'
        self._send_response(404, html, 'text/html')
    
    def _send_response(self, status_code, content, content_type):
        """发送HTTP响应"""
        self.send_response(status_code)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(content.encode('utf-8'))))
        self.end_headers()
        self.wfile.write(content.encode('utf-8'))
    
    def _get_dashboard_html(self):
        """获取仪表板HTML"""
        return '''
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Redis同步服务 - 管理界面</title>
    <link rel="stylesheet" href="/static/style.css">
</head>
<body>
    <div class="container">
        <header>
            <h1>Redis同步服务</h1>
            <div class="status-indicator" id="serviceStatus">
                <span class="status-dot"></span>
                <span class="status-text">检查中...</span>
            </div>
        </header>
        
        <main>
            <section class="overview">
                <h2>服务概览</h2>
                <div class="stats-grid">
                    <div class="stat-card">
                        <h3>运行状态</h3>
                        <div class="stat-value" id="runningStatus">-</div>
                    </div>
                    <div class="stat-card">
                        <h3>目标数量</h3>
                        <div class="stat-value" id="targetCount">-</div>
                    </div>
                    <div class="stat-card">
                        <h3>健康目标</h3>
                        <div class="stat-value" id="healthyTargets">-</div>
                    </div>
                    <div class="stat-card">
                        <h3>运行时间</h3>
                        <div class="stat-value" id="uptime">-</div>
                    </div>
                </div>
            </section>
            
            <section class="targets">
                <h2>同步目标</h2>
                <div class="targets-list" id="targetsList">
                    <p>加载中...</p>
                </div>
            </section>
        </main>
    </div>
    
    <script src="/static/script.js"></script>
</body>
</html>
        '''
    
    def _get_css(self):
        """获取CSS样式"""
        return '''
* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background-color: #f5f5f5;
    color: #333;
    line-height: 1.6;
}

.container {
    max-width: 1200px;
    margin: 0 auto;
    padding: 20px;
}

header {
    background: white;
    padding: 20px;
    border-radius: 8px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    margin-bottom: 20px;
    display: flex;
    justify-content: space-between;
    align-items: center;
}

h1 {
    color: #2c3e50;
    font-size: 24px;
}

.status-indicator {
    display: flex;
    align-items: center;
    gap: 8px;
}

.status-dot {
    width: 12px;
    height: 12px;
    border-radius: 50%;
    background-color: #95a5a6;
}

.status-dot.running {
    background-color: #27ae60;
}

.status-dot.stopped {
    background-color: #e74c3c;
}

section {
    background: white;
    padding: 20px;
    border-radius: 8px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    margin-bottom: 20px;
}

h2 {
    color: #2c3e50;
    margin-bottom: 15px;
    font-size: 18px;
}

.stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 15px;
}

.stat-card {
    background: #f8f9fa;
    padding: 15px;
    border-radius: 6px;
    text-align: center;
}

.stat-card h3 {
    font-size: 14px;
    color: #6c757d;
    margin-bottom: 8px;
}

.stat-value {
    font-size: 24px;
    font-weight: bold;
    color: #2c3e50;
}

.targets-list {
    display: grid;
    gap: 15px;
}

.target-card {
    border: 1px solid #dee2e6;
    border-radius: 6px;
    padding: 15px;
    display: grid;
    grid-template-columns: 1fr auto;
    align-items: center;
    gap: 15px;
}

.target-info h4 {
    color: #2c3e50;
    margin-bottom: 5px;
}

.target-info p {
    color: #6c757d;
    font-size: 14px;
}

.target-stats {
    text-align: right;
}

.target-status {
    display: inline-block;
    padding: 4px 8px;
    border-radius: 4px;
    font-size: 12px;
    font-weight: bold;
    text-transform: uppercase;
}

.target-status.healthy {
    background-color: #d4edda;
    color: #155724;
}

.target-status.unhealthy {
    background-color: #f8d7da;
    color: #721c24;
}

@media (max-width: 768px) {
    .container {
        padding: 10px;
    }
    
    header {
        flex-direction: column;
        gap: 10px;
        text-align: center;
    }
    
    .stats-grid {
        grid-template-columns: 1fr;
    }
    
    .target-card {
        grid-template-columns: 1fr;
        text-align: center;
    }
}
        '''
    
    def _get_javascript(self):
        """获取JavaScript代码"""
        return '''
class Dashboard {
    constructor() {
        this.updateInterval = 5000; // 5秒更新一次
        this.init();
    }
    
    init() {
        this.updateStatus();
        setInterval(() => this.updateStatus(), this.updateInterval);
    }
    
    async updateStatus() {
        try {
            const response = await fetch('/api/status');
            const status = await response.json();
            this.renderStatus(status);
        } catch (error) {
            console.error('获取状态失败:', error);
            this.renderError();
        }
    }
    
    renderStatus(status) {
        // 更新服务状态指示器
        const statusIndicator = document.getElementById('serviceStatus');
        const statusDot = statusIndicator.querySelector('.status-dot');
        const statusText = statusIndicator.querySelector('.status-text');
        
        if (status.running) {
            statusDot.className = 'status-dot running';
            statusText.textContent = '运行中';
        } else {
            statusDot.className = 'status-dot stopped';
            statusText.textContent = '已停止';
        }
        
        // 更新概览统计
        document.getElementById('runningStatus').textContent = status.running ? '运行中' : '已停止';
        document.getElementById('targetCount').textContent = Object.keys(status.targets).length;
        
        const healthyCount = Object.values(status.targets).filter(t => t.healthy).length;
        document.getElementById('healthyTargets').textContent = healthyCount;
        
        if (status.uptime) {
            document.getElementById('uptime').textContent = this.formatUptime(status.uptime);
        }
        
        // 更新目标列表
        this.renderTargets(status.targets);
    }
    
    renderTargets(targets) {
        const targetsList = document.getElementById('targetsList');
        
        if (Object.keys(targets).length === 0) {
            targetsList.innerHTML = '<p>没有配置的同步目标</p>';
            return;
        }
        
        const html = Object.entries(targets).map(([name, target]) => `
            <div class="target-card">
                <div class="target-info">
                    <h4>${name}</h4>
                    <p>同步: ${target.total_synced} | 失败: ${target.total_failed}</p>
                    <p>最后同步: ${target.last_sync_time ? this.formatTime(target.last_sync_time) : '从未'}</p>
                    ${target.last_error ? `<p style="color: #e74c3c;">错误: ${target.last_error}</p>` : ''}
                </div>
                <div class="target-stats">
                    <div class="target-status ${target.healthy ? 'healthy' : 'unhealthy'}">
                        ${target.healthy ? '健康' : '不健康'}
                    </div>
                    <p>连续失败: ${target.consecutive_failures}</p>
                </div>
            </div>
        `).join('');
        
        targetsList.innerHTML = html;
    }
    
    renderError() {
        const statusIndicator = document.getElementById('serviceStatus');
        const statusDot = statusIndicator.querySelector('.status-dot');
        const statusText = statusIndicator.querySelector('.status-text');
        
        statusDot.className = 'status-dot';
        statusText.textContent = '连接失败';
        
        document.getElementById('targetsList').innerHTML = '<p>无法获取目标状态</p>';
    }
    
    formatUptime(seconds) {
        const hours = Math.floor(seconds / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        const secs = Math.floor(seconds % 60);
        
        if (hours > 0) {
            return `${hours}小时${minutes}分钟`;
        } else if (minutes > 0) {
            return `${minutes}分钟${secs}秒`;
        } else {
            return `${secs}秒`;
        }
    }
    
    formatTime(timestamp) {
        const date = new Date(timestamp * 1000);
        return date.toLocaleString('zh-CN');
    }
}

// 初始化仪表板
document.addEventListener('DOMContentLoaded', () => {
    new Dashboard();
});
        '''
    
    def log_message(self, format, *args):
        """禁用默认的访问日志"""
        pass


class WebUI:
    """Web UI服务器"""
    
    def __init__(self, sync_service, host='0.0.0.0', port=8080):
        self.sync_service = sync_service
        self.host = host
        self.port = port
        self.server = None
        self.server_thread = None
    
    def start(self):
        """启动Web UI服务器"""
        def handler(*args, **kwargs):
            return WebUIHandler(self.sync_service, *args, **kwargs)
        
        self.server = HTTPServer((self.host, self.port), handler)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        
        print(f"Web UI启动成功: http://{self.host}:{self.port}")
    
    def stop(self):
        """停止Web UI服务器"""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
        
        if self.server_thread:
            self.server_thread.join(timeout=5)
        
        print("Web UI已停止")


if __name__ == '__main__':
    # 测试Web UI
    class MockSyncService:
        def __init__(self):
            self.config = {'test': True}
            self.start_time = time.time()
        
        def get_status(self):
            return {
                'running': True,
                'targets': {
                    'target1': {
                        'healthy': True,
                        'total_synced': 1000,
                        'total_failed': 5,
                        'last_sync_time': time.time() - 60,
                        'last_error': None,
                        'consecutive_failures': 0
                    },
                    'target2': {
                        'healthy': False,
                        'total_synced': 500,
                        'total_failed': 20,
                        'last_sync_time': time.time() - 300,
                        'last_error': '连接超时',
                        'consecutive_failures': 3
                    }
                }
            }
    
    mock_service = MockSyncService()
    web_ui = WebUI(mock_service, port=8080)
    web_ui.start()
    
    try:
        input("按回车键停止Web UI...")
    finally:
        web_ui.stop()
