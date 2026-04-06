"""
Redis RESP 协议解析（复制流等场景使用的子集）。
"""

from typing import List, Optional, Tuple


def parse_resp_array_command(data: bytes) -> Tuple[Optional[List[bytes]], bytes]:
    """
    解析 RESP 数组格式的命令。

    返回: (命令参数列表, 剩余未消费的字节)；数据不完整时返回 (None, data)。
    """
    if not data or data[0:1] != b'*':
        return None, data

    try:
        crlf_pos = data.find(b'\r\n')
        if crlf_pos == -1:
            return None, data

        array_len = int(data[1:crlf_pos])
        pos = crlf_pos + 2

        if array_len < 0:
            return None, data[pos:]

        elements: List[bytes] = []
        for _ in range(array_len):
            if pos >= len(data) or data[pos : pos + 1] != b'$':
                return None, data

            crlf_pos = data.find(b'\r\n', pos)
            if crlf_pos == -1:
                return None, data

            elem_len = int(data[pos + 1 : crlf_pos])
            pos = crlf_pos + 2

            if pos + elem_len + 2 > len(data):
                return None, data

            element = data[pos : pos + elem_len]
            elements.append(element)
            pos += elem_len + 2

        return elements, data[pos:]

    except (ValueError, IndexError):
        return None, data
