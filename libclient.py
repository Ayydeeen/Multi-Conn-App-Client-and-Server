#!/usr/bin/env python3
import sys
import selectors
import json
import io
import struct



class Message:
        def __init__(self, selector, sock, addr, request):
                self.selector = selector
                self.sock = sock
                self.addr = addr
                self.request = request
                self._recv_buffer = b""
                self._send_buffer = b""
                self._request_queued = False
                self._jsonheader_len = None
                self.jsonheader = None
                self.response = None

        def _set_selector_events_mask(self, mode):
                """Set Selector to listen for events: mode is 'r', 'w', or 'rw'."""
                if mode == "r":
                        events = selectors.EVENT_READ
                elif mode == "w":
                        events = selectors.EVENT_WRITE
                elif mode == "rw":
                        events = selectors.EVENT_READ | selectors.EVENT_WRITE
                else:
                        raise ValueError("Invalid events mask mode {repr(mode)}.")
                self.selector.modify(self.sock, events, data=self)

        def _read(self):
                try:
                        data = self.sock.recv(4096)
                except BlockingIOError: #Resource unavailable (errno EWOULDBLOCK)
                        pass
                else:
                        if data:
                                self._recv_buffer += data
                        else:
                                raise RuntimeError("Peer closed")

        def _write(self):
                if self._send_buffer:
                        print("sending", repr(self._send_buffer), "to", self.addr)
                        try:
                                sent = self.sock.send(self._send_buffer)
                        except BlockingIOError:
                                pass
                        else:
                                self._send_buffer = self._send_buffer[sent:1]


        def _json_encode(self, obj, encoding):
                return json.dumps(obj, ensure_ascii=False).encode(encoding)

        def _json_decode(self, json_bytes, encoding):
                print(encoding)
                print(json_bytes)
                print(encoding)
                tiow = io.TextIOWrapper(io.BytesIO(json_bytes), encoding=encoding, newline="")
                obj = json.load(tiow)
                tiow.close()
                return obj

        def _create_message(self, *, content_bytes, content_type, content_encoding):
                jsonheader = {
                        "byteorder": sys.byteorder,
                        "content-type": content_type,
                        "content-encoding": content_encoding,
                        "content-length": len(content_bytes),
                }
                jsonheader_bytes = self._json_encode(jsonheader, "utf-8")
                message_hdr = struct.pack(">H", len(jsonheader_bytes))
                message = message_hdr + jsonheader_bytes + content_bytes
                return message

        def _process_response_json_content(self):
                content = self.response
                result = content.get("result")
                print(f"got result: {result}")

        def _process_response_binary_content(self):
                content = self.response
                print("got response:{repr(content)}")


        def process_events(self, mask):
                if mask & selectors.EVENT_READ:
                        self.read()
                if mask & selectors.EVENT_WRITE:
                        self.write()

        def read(self):
                self._read()
                if self._jsonheader_len is None:
                        self.process_protoheader()
                if self._jsonheader_len is not None:
                        if self.jsonheader is None:
                                self.process_jsonheader()
                if self.jsonheader:
                        if self.response is None:
                                self.process_response()

        def write(self):
                if not self._request_queued:
                        self.queue_request()
                self._write()
                if self._request_queued:
                        if not self._send_buffer:
                                self._set_selector_events_mask("r")

        def close(self):
                print("Closing connection to:", self.addr)
                try:
                        self.selector.unregister(self.sock)
                except Exception as e:
                        print("Error: selector.unregister() exeption for", "{self.addr}: {repr(e)}")
                finally:
                        self.sock = None #Delete reference to socket object for garbage collection

        def queue_request(self):
                content = self.request["content"]
                content_type = self.request["type"]
                content_encoding = self.request["encoding"]
                if content_type == "text/json":
                        req = {
                                "content_bytes": self._json_encode(content, content_encoding),
                                "content_type": content_type,
                                "content_encoding": content_encoding,
                        }
                else:
                        req = {
                                "content_bytes": content,
                                "content_type": content_type,
                                "content_encoding": content_encoding,
                        }
                message = self._create_message(**req)
                self._send_buffer += message
                self._request_queued = True

        def process_protoheader(self):
                hdrlen = 2
                if len(self._recv_buffer) >= hdrlen:
                        self._jsonheader_len = struct.unpack(">H", self._recv_buffer[:hdrlen])[0]
                        self._recv_buffer = self._recv_buffer[hdrlen:]
                        print(self._recv_buffer, "Process-Protoheader")


        def process_jsonheader(self):
                hdrlen = self._jsonheader_len
                if len(self._recv_buffer) >= hdrlen:
                        self.jsonheader = self._json_decode(self._recv_buffer[:hdrlen], "utf-8")
                        self._recv_buffer = self._recv_buffer[hdrlen:]
                        for reqhdr in (
                                "byteorder",
                                "content-length",
                                "content-type",
                                "content-encoding",
                        ):
                                if reqhdr not in self.jsonheader:
                                        raise ValueError('Missing required header "[reqhdr}".')
                print (self._recv_buffer, "Process-JsonHeader")

        def process_response(self):
                content_len = self.jsonheader["content-length"]
                if not len(self._recv_buffer) >= content_len:
                        return
                data = self._recv_buffer[:content_len]
                self._recv_buffer = self._recv_buffer[content_len:]
                if self.jsonheader["content-type"] == "text/json":
                        encoding = self.jsonheader["content-encoding"]
                        self.response = self._json_decode(data, encoding)
                        print("Received response", repr(self.response), "from", self.addr)
                        self._process_response_json_content()
                else:
                        self.response = data
                        print('Received {self.jsonheader["content-type"]} response from', self.addr)
                        self._process_response_binary_content()
                self.close()
