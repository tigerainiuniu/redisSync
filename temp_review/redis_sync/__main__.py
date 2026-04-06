"""支持 python -m redis_sync 启动同步服务（与 sync_service.main 一致）。"""

from .sync_service import main

if __name__ == "__main__":
    main()
