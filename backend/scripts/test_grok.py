#!/usr/bin/env python3
"""Quick test for xAI Grok API key & model availability."""
import os, urllib.request, json

API_KEY = os.getenv("XAI_API_KEY", "your-xai-key-here")
BASE_URL = "https://api.x.ai/v1"

print("=" * 50)
print("Step 1: Listing available models")
print("=" * 50)
req = urllib.request.Request(
    f"{BASE_URL}/models",
    headers={"Authorization": f"Bearer {API_KEY}"},
)
try:
    resp = urllib.request.urlopen(req, timeout=15)
    body = json.loads(resp.read().decode())
    models = body.get("data", [])
    print(f"OK — {len(models)} models available:\n")
    for m in models:
        print(f"  {m.get('id', '?')}")
except urllib.error.HTTPError as e:
    err = e.read().decode()[:500]
    print(f"FAILED — HTTP {e.code}: {err}")
except Exception as e:
    print(f"FAILED — {e}")

print("\n" + "=" * 50)
print("Step 2: Chat completion with grok-4.3")
print("=" * 50)
payload = {
    "model": "grok-4.3",
    "messages": [
        {"role": "user", "content": "Say hello in exactly 3 words."}
    ],
    "max_tokens": 20,
}
data = json.dumps(payload).encode()
req = urllib.request.Request(
    f"{BASE_URL}/chat/completions",
    data=data,
    headers={
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    },
)
try:
    resp = urllib.request.urlopen(req, timeout=20)
    body = json.loads(resp.read().decode())
    print(f"OK — model={body.get('model')}")
    print(f"  Response: {body['choices'][0]['message']['content']}")
except urllib.error.HTTPError as e:
    err = e.read().decode()[:500]
    print(f"FAILED — HTTP {e.code}: {err}")
except Exception as e:
    print(f"FAILED — {e}")

print("\nDone.")
