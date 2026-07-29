"""Minimal stdlib WebSocket client (RFC 6455, no external deps)
+ FinancialJuice Centrifugo bootstrap (homepage scrape for token/cookies).
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import random
import re
import ssl
import struct
import urllib.request
from typing import Dict


# ---------------------------------------------------------------------------
# Minimal stdlib WebSocket client  (RFC 6455, no external deps)
# ---------------------------------------------------------------------------

class _StdlibWebSocket:
    """
    Bare-bones async WebSocket client using only asyncio + ssl.

    Handles:
      - TLS handshake via ssl.create_default_context
      - HTTP Upgrade handshake (Sec-WebSocket-Key, 101 response)
      - RFC 6455 frame encode / decode (text frames only)
      - Graceful close (opcode 0x8)
    """

    _GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

    def __init__(self) -> None:
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self.close_info: str | None = None  # populated when server sends close frame

    async def connect(
        self,
        url: str,
        *,
        extra_headers: Dict[str, str] | None = None,
        timeout: float = 15.0,
    ) -> None:
        # ---- parse URL ----
        if not url.startswith("wss://"):
            raise ValueError("only wss:// is supported")
        rest = url[6:]
        if ":" in rest.split("/")[0]:
            host, port_str = rest.split("/")[0].split(":", 1)
            port = int(port_str)
        else:
            host = rest.split("/")[0]
            port = 443
        path = "/" + rest.split("/", 1)[1] if "/" in rest else "/"

        # ---- TLS + TCP ----
        ctx = ssl.create_default_context()
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, ssl=ctx),
            timeout=timeout,
        )
        self._reader = reader
        self._writer = writer

        # ---- WebSocket upgrade handshake ----
        key_bytes = bytes(random.getrandbits(8) for _ in range(16))
        key_b64 = base64.b64encode(key_bytes).decode()
        accept = base64.b64encode(
            hashlib.sha1((key_b64 + self._GUID.decode()).encode()).digest()
        ).decode()

        req_lines = [
            f"GET {path} HTTP/1.1",
            f"Host: {host}",
            "Upgrade: websocket",
            "Connection: Upgrade",
            f"Sec-WebSocket-Key: {key_b64}",
            "Sec-WebSocket-Version: 13",
        ]
        if extra_headers:
            for k, v in extra_headers.items():
                req_lines.append(f"{k}: {v}")
        req_lines.append("")  # blank line
        req_lines.append("")

        writer.write("\r\n".join(req_lines).encode())
        await writer.drain()

        # Read 101 response
        status_line = await asyncio.wait_for(
            reader.readline(), timeout=timeout
        )
        status = status_line.decode(errors="replace").strip()
        if "101" not in status:
            # drain headers
            while True:
                line = await asyncio.wait_for(
                    reader.readline(), timeout=timeout
                )
                if line.strip() == b"":
                    break
            raise ConnectionError(f"WebSocket upgrade rejected: {status}")

        # Read response headers
        resp_accept = ""
        while True:
            line = await asyncio.wait_for(
                reader.readline(), timeout=timeout
            )
            if line.strip() == b"":
                break
            decoded = line.decode(errors="replace").strip()
            if decoded.lower().startswith("sec-websocket-accept:"):
                resp_accept = decoded.split(":", 1)[1].strip()

        if resp_accept != accept:
            raise ConnectionError(
                f"Sec-WebSocket-Accept mismatch: expected {accept}, got {resp_accept}"
            )

    async def recv_text(self, timeout: float = 300.0) -> str | None:
        """Receive and decode one text frame. Returns None on close frame."""
        assert self._reader is not None
        while True:
            data = await self._read_frame(timeout)
            if data is None:
                return None  # close frame
            if isinstance(data, str):
                return data
            # binary / ping / pong — continue

    async def send_text(self, text: str) -> None:
        await self._send_frame(0x1, text.encode("utf-8"))

    async def close(self) -> None:
        if self._writer is None:
            return
        try:
            await self._send_frame(0x8, b"")
        except Exception:
            pass
        try:
            self._writer.close()
            await self._writer.wait_closed()
        except Exception:
            pass
        self._reader = None
        self._writer = None

    # ---- internal RFC 6455 framing ----

    async def _read_frame(self, timeout: float) -> str | bytes | None:
        reader = self._reader
        assert reader is not None

        # 2-byte header minimum
        hdr = await asyncio.wait_for(reader.readexactly(2), timeout=timeout)
        b0, b1 = hdr[0], hdr[1]
        fin = bool(b0 & 0x80)
        opcode = b0 & 0x0F
        masked = bool(b1 & 0x80)
        length = b1 & 0x7F

        if length == 126:
            ext = await asyncio.wait_for(reader.readexactly(2), timeout=timeout)
            length = struct.unpack("!H", ext)[0]
        elif length == 127:
            ext = await asyncio.wait_for(reader.readexactly(8), timeout=timeout)
            length = struct.unpack("!Q", ext)[0]

        mask_key = (
            await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
            if masked
            else b""
        )
        payload = await asyncio.wait_for(reader.readexactly(length), timeout=timeout)

        if masked:
            payload = bytes(
                b ^ mask_key[i % 4] for i, b in enumerate(payload)
            )

        # Handle opcodes
        if opcode == 0x8:  # close
            # Decode close reason: 2-byte status code + optional UTF-8 message
            if len(payload) >= 2:
                code = struct.unpack("!H", payload[:2])[0]
                reason = payload[2:].decode("utf-8", errors="replace")
                self.close_info = f"code={code} reason={reason}" if reason else f"code={code}"
            else:
                self.close_info = "no reason"
            return None
        if opcode == 0x9:  # ping → pong
            await self._send_frame(0xA, payload)
            return b""  # skip, caller loops
        if opcode == 0xA:  # pong
            return b""
        if opcode in (0x1, 0x0):  # text or continuation
            result = payload.decode("utf-8", errors="replace")
            if fin:
                return result
            # For continuation frames, accumulate (simplified: return per-frame)
            return result
        if opcode == 0x2:  # binary
            return payload

        return b""  # unknown opcode — skip

    async def _send_frame(self, opcode: int, payload: bytes) -> None:
        assert self._writer is not None
        frame = bytearray()
        frame.append(0x80 | opcode)

        plen = len(payload)
        # RFC 6455: client MUST mask all frames. Set MASK bit + mask key.
        mask_bit = 0x80
        mask_key = bytes(random.getrandbits(8) for _ in range(4))
        masked_payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))

        if plen < 126:
            frame.append(mask_bit | plen)
        elif plen < 65536:
            frame.append(mask_bit | 126)
            frame.extend(struct.pack("!H", plen))
        else:
            frame.append(mask_bit | 127)
            frame.extend(struct.pack("!Q", plen))

        frame.extend(mask_key)
        frame.extend(masked_payload)
        self._writer.write(bytes(frame))
        await self._writer.drain()

    @property
    def closed(self) -> bool:
        return self._writer is None or self._writer.is_closing()


# ---------------------------------------------------------------------------
# Task A — FinancialJuice WebSocket Ingest  (Centrifugo protocol)
# ---------------------------------------------------------------------------

# Shared headers with authentication cookies — used for both homepage scrape
# and WebSocket handshake to prevent server-side kick.
_FJ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cookie": "ASP.NET_SessionId=qnqyymqq3fqp5xavxwa2mlat; _gid=GA1.2.1581817587.1783567004; _twpid=tw.1783567005955.474360820353453615; FJ-UID=734613; FJ-Email=strawberrytiptip@gmail.com; .ASPXAUTH=40D2C2BCD923C8AF4F252765F9CE31D95F93AE1D63FE64B9479FCF9E513328138F223A72ECED1F0B0ED4CC9FA88ED244E1083C647EBB1CFE39E5EA9D216E22D7A8271A9C7F0617BD97FF43F515E44898F54531EA581E1B41C7ABC07E87E9EDE210723CEFBCB681D62A7E35799DE745E0CB5A3E82; FJ-Pop=show; FJ-UName=Srawberry; cf_clearance=a17ZdHHHit2gjy7NjRBn4wbaAyJEajMLpQvtYNtXrBA-1783567094-1.2.1.1-7N1hDiKJeI7L4ARZxpTdV7.rglYfieL20fpeucevtTB06uDGA3TZHUU5HbuevkawKg_fK2UAluOfMnngdUFOAaQCj2R5VUPr1uf0eVpYOHyvR_MJTBC8uxIotkvdxMAAtrYKpfLzhK7IofukdckEi9YEhal7Plnhdi.m2X4HNxnLcezrJdGFTLraA9F5MVcsKd4.XioPCsB2TIYVe0x7ZTHyK9Wz6afI6JsqPk5n53nA.AWcPfpyFF8a.twXleLoD4M9T0akTpz0ah_uT6wybXYnjAeHL_lT_lx5hin_ZwOXP3wGFleDdYmeAyIb7kATinR4Wb.a1BE0djecs2_jHAPUxjhLhi9_owLj_IPzDDzhSo6egIExywHugJ0.7lWHedbS9oWyOiqPPR8GSxH_BydVuDFjEazPn6XtPsDvuzOlYqlyVpLK49ZXQFECOSkthSoTlBJbDETC.JOM23qX6fKY0EL6lNG8dimqvSmPaPpOwAIc64cQVlij.yJ6JWbK; _gat=1; _ga_MWM91XTKTP=GS2.1.s1783571691$o2$g1$t1783571705$j46$l0$h0; _ga=GA1.2.2144357452.1783567003",
}


def _extract_centrifugo_config() -> dict:
    """
    Scrape the FinancialJuice homepage for Centrifugo JS variables AND
    capture fresh Set-Cookie headers from the response.
    The fresh cookies are essential for the WebSocket connection because
    Centrifugo is proxied through ASP.NET which validates .ASPXAUTH.
    Returns a dict with keys: token, cookies, centrifugoUrl, etc.
    """
    req = urllib.request.Request(
        "https://www.financialjuice.com/",
        headers=_FJ_HEADERS,
    )
    proxy_handler = urllib.request.ProxyHandler({})
    opener = urllib.request.build_opener(proxy_handler)
    try:
        resp = opener.open(req, timeout=15)
        html = resp.read().decode("utf-8", errors="replace")
    except Exception as exc:
        print(f"  [INGEST] Homepage fetch failed: {exc}")
        return {}

    config: dict = {}

    # Capture fresh Set-Cookie headers from the response
    set_cookie_headers = resp.headers.get_all("Set-Cookie") if hasattr(resp.headers, "get_all") else []
    if not set_cookie_headers:
        # Fallback: try the singular form
        sc = resp.headers.get("Set-Cookie")
        if sc:
            set_cookie_headers = [sc]
    fresh_cookies: list[str] = []
    for h in set_cookie_headers:
        # Extract just the name=value part (before first ;)
        parts = h.split(";")
        if parts:
            fresh_cookies.append(parts[0].strip())
    if fresh_cookies:
        config["cookies"] = "; ".join(fresh_cookies)

    # 1) Token — var centrifugoToken = '...' or "..."
    for pat in (r"var centrifugoToken\s*=\s*'([^']+)'",
                r'var centrifugoToken\s*=\s*"([^"]+)"'):
        m = re.search(pat, html)
        if m:
            config["token"] = m.group(1)
            break

    # 2) centrifugoUrl
    for pat in (r"var centrifugoUrl\s*=\s*'([^']+)'",
                r'var centrifugoUrl\s*=\s*"([^"]+)"'):
        m = re.search(pat, html)
        if m:
            config["centrifugoUrl"] = m.group(1)
            break

    # 3) Look for inline script blocks mentioning centrifuge init
    for m in re.finditer(
        r"<script[^>]*>([\s\S]*?)</script>", html, re.IGNORECASE
    ):
        body = m.group(1)
        if "centrifugo" in body.lower() or "centrifuge" in body.lower():
            idx2 = body.lower().find("centrifugo")
            if idx2 < 0:
                idx2 = body.lower().find("centrifuge")
            snippet = body[max(0, idx2 - 60):idx2 + 500]
            config.setdefault("_script_snippets", []).append(snippet.strip())

    return config
