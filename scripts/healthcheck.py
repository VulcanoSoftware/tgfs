"""Healthcheck for the tgfs backend: POST /login and accept any HTTP response."""
import sys
import urllib.error
import urllib.request

try:
    req = urllib.request.Request(
        "http://localhost:1900/login",
        data=b'{"username":"","password":""}',
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=3)
except urllib.error.HTTPError:
    # Any HTTP error (401, 422, etc.) means the server is responding.
    pass
except Exception:
    sys.exit(1)
