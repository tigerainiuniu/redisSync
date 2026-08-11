#!/usr/bin/env python3
"""
Redis同步服务Web管理界面

提供简单的Web界面来监控和管理Redis同步服务。
"""

import base64
import binascii
import hmac
import ipaddress
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


DEFAULT_MAX_REQUEST_WORKERS = 32


_SENSITIVE_CONFIG_FIELDS = {
    'access_key',
    'access_key_id',
    'access_token',
    'api_key',
    'authorization',
    'credential',
    'credentials',
    'dsn',
    'encryption_key',
    'key',
    'password',
    'passwd',
    'private_key',
    'refresh_token',
    'secret',
    'secret_key',
    'token',
    'uri',
    'url',
}


def _is_sensitive_config_field(field_name):
    normalized = str(field_name).strip().lower().replace('-', '_')
    if normalized in _SENSITIVE_CONFIG_FIELDS:
        return True
    return normalized.endswith(
        ('_password', '_secret', '_token', '_api_key', '_url', '_uri', '_dsn')
    )


def _redact_sensitive_config(value):
    """Return a recursively redacted copy suitable for the config API."""
    if isinstance(value, dict):
        redacted = {}
        for key, item in value.items():
            if _is_sensitive_config_field(key):
                redacted[key] = '***' if item else item
            else:
                redacted[key] = _redact_sensitive_config(item)
        return redacted
    if isinstance(value, list):
        return [_redact_sensitive_config(item) for item in value]
    if isinstance(value, tuple):
        return [_redact_sensitive_config(item) for item in value]
    return value


class WebUIHandler(BaseHTTPRequestHandler):
    """Web UI请求处理器"""
    
    def __init__(self, sync_service, *args, **kwargs):
        self.sync_service = sync_service
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """处理GET请求"""
        if not self._authorize_request():
            return

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
        if not self._authorize_request():
            return

        parsed_path = urlparse(self.path)
        path = parsed_path.path
        
        if path == '/api/reload':
            self._handle_reload()
        else:
            self._serve_404()

    def _authorize_request(self):
        """Apply configured client IP and API key restrictions."""
        config = getattr(self.sync_service, 'config', {})
        security = config.get('security', {}) if isinstance(config, dict) else {}
        security = security if isinstance(security, dict) else {}

        allowed_ips = security.get('allowed_ips')
        if isinstance(allowed_ips, str):
            allowed_ips = [allowed_ips]

        client_address = getattr(self, 'client_address', ('', 0))
        client_ip = client_address[0] if client_address else ''
        if allowed_ips and not self._client_ip_allowed(client_ip, allowed_ips):
            self._send_response(
                403,
                json.dumps({'success': False, 'message': '来源地址未获准访问'}),
                'application/json',
            )
            return False

        if not security.get('auth_enabled', False):
            return True

        expected_api_key = security.get('api_key')
        if not expected_api_key:
            self._send_response(
                503,
                json.dumps({'success': False, 'message': 'Web API 认证配置缺少 api_key'}),
                'application/json',
            )
            return False

        headers = getattr(self, 'headers', {})
        provided_api_key = headers.get('X-API-Key') if headers else None
        if not provided_api_key and headers:
            authorization = headers.get('Authorization', '')
            scheme, separator, credentials = authorization.partition(' ')
            if separator and scheme.lower() == 'bearer':
                provided_api_key = credentials.strip()
            elif separator and scheme.lower() == 'basic':
                try:
                    decoded = base64.b64decode(
                        credentials.strip(), validate=True
                    ).decode('utf-8')
                    _username, colon, password = decoded.partition(':')
                    provided_api_key = password if colon else decoded
                except (binascii.Error, UnicodeDecodeError, ValueError):
                    provided_api_key = None

        if not provided_api_key or not hmac.compare_digest(
            str(provided_api_key).encode('utf-8'),
            str(expected_api_key).encode('utf-8'),
        ):
            self._send_response(
                401,
                json.dumps({'success': False, 'message': 'API key 校验失败'}),
                'application/json',
                headers={'WWW-Authenticate': 'Basic realm="Redis Sync"'},
            )
            return False

        return True

    @staticmethod
    def _client_ip_allowed(client_ip, allowed_ips):
        """Match exact IP addresses or CIDR networks without trusting proxy headers."""
        try:
            parsed_client_ip = ipaddress.ip_address(str(client_ip).split('%', 1)[0])
        except ValueError:
            return False

        if isinstance(parsed_client_ip, ipaddress.IPv6Address):
            parsed_client_ip = parsed_client_ip.ipv4_mapped or parsed_client_ip

        for allowed in allowed_ips:
            allowed_text = str(allowed).strip()
            if allowed_text == '*':
                return True
            try:
                network = ipaddress.ip_network(allowed_text, strict=False)
            except ValueError:
                continue
            if parsed_client_ip in network:
                return True
        return False
    
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
        """提供经过递归脱敏的配置API。"""
        config = _redact_sensitive_config(self.sync_service.config)

        self._send_response(200, json.dumps(config, indent=2), 'application/json')
    
    def _handle_reload(self):
        """明确报告当前服务尚未实现热加载。"""
        result = {'success': False, 'message': '配置热加载尚未实现，请重启服务'}
        self._send_response(501, json.dumps(result), 'application/json')
    
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
    
    def _send_response(self, status_code, content, content_type, headers=None):
        """发送HTTP响应"""
        self.send_response(status_code)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(content.encode('utf-8'))))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
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

.status-dot.degraded {
    background-color: #f39c12;
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
function escapeHtml(text) {
    if (text === null || text === undefined) return '';
    const div = document.createElement('div');
    div.textContent = String(text);
    return div.innerHTML;
}

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
        
        if (status.running && status.healthy === true) {
            statusDot.className = 'status-dot running';
            statusText.textContent = '运行健康';
        } else if (status.running) {
            statusDot.className = 'status-dot degraded';
            statusText.textContent = '运行异常';
        } else {
            statusDot.className = 'status-dot stopped';
            statusText.textContent = '已停止';
        }
        
        // 更新概览统计
        document.getElementById('runningStatus').textContent = !status.running
            ? '已停止'
            : (status.healthy === true ? '运行中' : '异常');
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
        
        const html = Object.entries(targets).map(([name, target]) => {
            const safeName = escapeHtml(name);
            const safeErr = target.last_error ? escapeHtml(target.last_error) : '';
            return `
            <div class="target-card">
                <div class="target-info">
                    <h4>${safeName}</h4>
                    <p>同步: ${escapeHtml(target.total_synced)} | 失败: ${escapeHtml(target.total_failed)}</p>
                    <p>最后同步: ${target.last_sync_time ? escapeHtml(this.formatTime(target.last_sync_time)) : '从未'}</p>
                    ${target.last_error ? `<p style="color: #e74c3c;">错误: ${safeErr}</p>` : ''}
                </div>
                <div class="target-stats">
                    <div class="target-status ${target.healthy ? 'healthy' : 'unhealthy'}">
                        ${target.healthy ? '健康' : '不健康'}
                    </div>
                    <p>连续失败: ${escapeHtml(target.consecutive_failures)}</p>
                </div>
            </div>
        `;
        }).join('');
        
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


class _ConcurrentHTTPServer(ThreadingHTTPServer):
    """Serve clients independently with bounded request concurrency."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        *args,
        request_timeout=5.0,
        max_workers=DEFAULT_MAX_REQUEST_WORKERS,
        **kwargs,
    ):
        self.request_timeout = float(request_timeout)
        if self.request_timeout <= 0:
            raise ValueError("request_timeout must be greater than zero")
        if isinstance(max_workers, bool):
            raise ValueError("max_workers must be a positive integer")
        self.max_workers = int(max_workers)
        if self.max_workers <= 0 or self.max_workers != max_workers:
            raise ValueError("max_workers must be a positive integer")
        self._request_slots = threading.BoundedSemaphore(self.max_workers)
        super().__init__(*args, **kwargs)

    def get_request(self):
        request, client_address = super().get_request()
        try:
            request.settimeout(self.request_timeout)
        except Exception:
            request.close()
            raise
        return request, client_address

    def process_request(self, request, client_address):
        if not self._request_slots.acquire(blocking=False):
            try:
                request.sendall(
                    b"HTTP/1.1 503 Service Unavailable\r\n"
                    b"Connection: close\r\n"
                    b"Content-Length: 0\r\n"
                    b"Retry-After: 1\r\n\r\n"
                )
            except OSError:
                pass
            self.shutdown_request(request)
            return

        try:
            super().process_request(request, client_address)
        except BaseException:
            self._request_slots.release()
            self.shutdown_request(request)
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()


class WebUI:
    """Web UI服务器"""
    
    def __init__(
        self,
        sync_service,
        host='127.0.0.1',
        port=8080,
        request_timeout=5.0,
        max_request_workers=DEFAULT_MAX_REQUEST_WORKERS,
    ):
        self.sync_service = sync_service
        self.host = host
        self.port = port
        self.request_timeout = request_timeout
        self.max_request_workers = max_request_workers
        self.server = None
        self.server_thread = None
        self._stop_state_lock = threading.Lock()
        self._shutdown_helper = None
        self._close_helper = None
    
    def start(self):
        """启动Web UI服务器"""
        def handler(*args, **kwargs):
            return WebUIHandler(self.sync_service, *args, **kwargs)
        
        self.server = _ConcurrentHTTPServer(
            (self.host, self.port),
            handler,
            request_timeout=self.request_timeout,
            max_workers=self.max_request_workers,
        )
        self.server_thread = threading.Thread(
            target=self.server.serve_forever,
            kwargs={'poll_interval': 0.1},
            daemon=True,
        )
        self.server_thread.start()
        
        print(f"Web UI启动成功: http://{self.host}:{self.port}")
    
    def stop(self, timeout=5.0):
        """停止Web UI服务器"""
        deadline = time.monotonic() + max(0.0, float(timeout))

        def remaining():
            return max(0.0, deadline - time.monotonic())

        with self._stop_state_lock:
            server = self.server
            server_thread = self.server_thread
            shutdown_helper = self._shutdown_helper
            close_helper = self._close_helper

            if server is not None and shutdown_helper is None:
                shutdown_helper = threading.Thread(
                    target=server.shutdown,
                    name="redis-sync-web-shutdown",
                    daemon=True,
                )
                self._shutdown_helper = shutdown_helper
                shutdown_helper.start()

        if shutdown_helper is not None:
            shutdown_helper.join(timeout=remaining())

        with self._stop_state_lock:
            if server is not None and self._close_helper is None:
                close_helper = threading.Thread(
                    target=server.server_close,
                    name="redis-sync-web-close",
                    daemon=True,
                )
                self._close_helper = close_helper
                close_helper.start()
            else:
                close_helper = self._close_helper

        if close_helper is not None:
            close_helper.join(timeout=remaining())

        if server_thread is not None:
            try:
                server_thread.join(timeout=remaining())
            except RuntimeError:
                # A partially failed start can leave an unstarted thread object.
                pass

        shutdown_stopped = (
            shutdown_helper is None or not shutdown_helper.is_alive()
        )
        close_stopped = close_helper is None or not close_helper.is_alive()
        server_stopped = server_thread is None or not server_thread.is_alive()
        stopped = shutdown_stopped and close_stopped and server_stopped

        if stopped:
            with self._stop_state_lock:
                if self.server is server:
                    self.server = None
                    self.server_thread = None
                    self._shutdown_helper = None
                    self._close_helper = None

        if stopped:
            print("Web UI已停止")
        else:
            print("Web UI停止未完成")
        return stopped


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
