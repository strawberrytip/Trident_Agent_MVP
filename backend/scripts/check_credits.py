#!/usr/bin/env python3
"""Check OpenRouter credit balance."""
import os, urllib.request, json

OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY", "your-openrouter-key-here")

req = urllib.request.Request(
    "https://openrouter.ai/api/v1/auth/key",
    headers={"Authorization": f"Bearer {OPENROUTER_KEY}"},
)
try:
    resp = urllib.request.urlopen(req, timeout=15)
    body = json.loads(resp.read().decode())
    data = body.get("data", body)
    print("OpenRouter Key Info:")
    print(f"  Label:       {data.get('label', 'N/A')}")
    credits = data.get("credits")
    print(f"  Credits:     ${credits if credits is not None else 'N/A'}")
    print(f"  Usage:       ${data.get('usage', 'N/A')}")
    print(f"  Limit:       ${data.get('limit', 'N/A')}")
    print(f"  Disabled:    {data.get('disabled', 'N/A')}")
except urllib.error.HTTPError as e:
    err = e.read().decode()[:500]
    print(f"OpenRouter HTTP {e.code}: {err}")
except Exception as e:
    print(f"ERROR: {e}")
