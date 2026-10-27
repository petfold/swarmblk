#!/usr/bin/env python3
"""A development server for web/. It serves this directory on 127.0.0.1:8099,
or on the port given as its argument, and keeps what a page in test mode
posts, in out/: /log lines in log.jsonl, the last boot trace in trace.bin
and the last screenshot in shot.png."""
import http.server
import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(ROOT, "out")


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=ROOT, **kwargs)

    def end_headers(self):
        self.send_header("Cache-Control", "no-cache")  # so that rebuilt files load
        super().end_headers()

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        names = {"/log": "log.jsonl", "/trace": "trace.bin", "/shot": "shot.png"}
        if self.path not in names:
            self.send_error(404)
            return
        os.makedirs(OUT, exist_ok=True)
        if self.path == "/log":
            with open(os.path.join(OUT, names[self.path]), "ab") as f:
                f.write(body.replace(b"\n", b"\\n") + b"\n")
        else:
            with open(os.path.join(OUT, names[self.path]), "wb") as f:
                f.write(body)
        self.send_response(204)
        self.end_headers()

    def log_message(self, fmt, *args):
        pass


port = int(sys.argv[1]) if len(sys.argv) > 1 else 8099
http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
