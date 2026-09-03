#!/usr/bin/env python3
import hashlib
import hmac
import json
import os
import re
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

SECRET = b"demo-secret"
PORT = 9921

if os.environ.get("NO_COLOR"):
    RESET = BOLD = DIM = CYAN = GREEN = YELLOW = RED = ""
else:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    CYAN = "\033[36m"
    GREEN = "\033[1;32m"
    YELLOW = "\033[1;33m"
    RED = "\033[1;31m"

COLORS = {
    "transfer.settled": GREEN,
    "transfer.confirmed_pending": YELLOW,
    "transfer.failed": RED,
    "payment.settled": GREEN,
    "payment.failed": RED,
}


def colorize(payload):
    text = json.dumps(payload, indent=2)
    if CYAN:
        text = re.sub(r'^(\s*)"([^"]+)":', rf'\1{CYAN}"\2"{RESET}:', text, flags=re.M)
    return "\n".join("    " + line for line in text.splitlines())


class Hook(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("content-length", 0))
        body = self.rfile.read(length)
        presented = self.headers.get("x-companion-signature", "")
        expected = hmac.new(SECRET, body, hashlib.sha256).hexdigest()
        now = datetime.now().strftime("%H:%M:%S")
        if not hmac.compare_digest(presented, expected):
            print(f"{RED}[{now}] SIGNATURE MISMATCH{RESET} "
                  f"got={presented!r} body={body!r}", flush=True)
            self.send_response(400)
            self.end_headers()
            return
        try:
            ev = json.loads(body)
        except json.JSONDecodeError as e:
            print(f"{RED}[{now}] bad payload: {e}{RESET}", flush=True)
            self.send_response(400)
            self.end_headers()
            return
        etype = ev.get("event_type", "?")
        obj = ev.get("transfer") or ev.get("payment") or {}
        color = COLORS.get(etype, BOLD)
        print(flush=True)
        print(f"{color}[{now}] {etype:<28}{RESET} {DIM}sig OK (HMAC-SHA256){RESET}", flush=True)
        print(f"  {'asset':<9} {obj.get('asset_id')}", flush=True)
        print(f"  {'id':<9} {obj.get('id') or obj.get('payment_hash')}  "
              f"kind={obj.get('kind') or obj.get('direction')}", flush=True)
        print(f"  {'status':<9} {ev.get('previous_status')} -> "
              f"{color}{ev.get('new_status')}{RESET}", flush=True)
        print(f"{colorize(ev)}", flush=True)
        print(flush=True)
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    print(f"{BOLD}webhook sink on 127.0.0.1:{PORT}/hook{RESET}", flush=True)
    print("waiting for webhooks from the companion...", flush=True)
    HTTPServer(("127.0.0.1", PORT), Hook).serve_forever()
