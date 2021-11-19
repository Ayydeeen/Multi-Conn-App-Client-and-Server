import sys
import selectors
import json
import io
import struct

request_search = {
    "morpheus": "Follow the white rabbit. \U0001f430",
    "ring": "In the caves beneath the Misty Mountains. \U0001f48d",
    "\U0001f436": "\U0001f43e Playing ball! \U0001f3d0",
}

class Message:
    
    def __init__(self, selector, sock, addr): #Used to register w/ data from accept_wrapper() function
        self.selector = selector
        self.sock = sock
        self.addr = addr
        self._recv_buffer = b"" 
        self._send_buffer = b""
        self._jsonheader_len = None
        self.jsonheader = None
        self.request = None
        self.request_created = False

    def _set_selector_events_mask(self, mode):
        """Set Selector to listen for events: mode is 'r', 'w', or 'rw'."""
        if mode == "r":
            events = selectors.EVENT_READ
        elif mode == "w":
            events = selectors.EVENT_WRITE
        elif mode == "rw":
            events = selectors.EVENT_READ | selectors.EVENT_WRITE
        else:
            raise ValueError(f"Invalid events mask mode {repr(mode)}.")
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
                    raise RuntimeError("Peer closed.")

    def _write(self):
        if self._send_buffer:
            print("sending", repr(self._send_buffer), "to", self.addr)
            try:
                sent = self.sock.send(self._send_buffer)
            except BlockingIOError:
                pass
            else:
                self._send_buffer = self._send_buffer[sent:]
                if sent and not self._send_buffer:
                        self.close()

    def _json_encode(self, obj, encoding):
        return json.dumps(obj, ensure_ascii=False).encode(encoding)
    
    def _json_decode(self, json_bytes, encoding):
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
        print(jsonheader)
        jsonheader_bytes = self._json_encode(jsonheader, "utf-8")
        message_hdr = struct.pack(">H", len(jsonheader_bytes))
        message = message_hdr + jsonheader_bytes + content_bytes
        return message

    def _create_response_json_content(self):
        action = self.request.get("action")
        if action == "search":
            query = self.request.get("value")
            answer = request_search.get(query) or f'No match for "{query}".'
            content = {"result": answer}
        else:
            content = {"result": f'Error: invalid action "{action}".'}
        content_encoding = "utf-8"
        response = {
            "content_bytes": self._json_encode(content, content_encoding),
            "content_type": "text/json",
            "content_encoding": content_encoding,
        }
        return response

    def _create_response_binary_content(self):
        response = {
            "content_bytes": b"First 10 bytes of request: "+ self.request[:10],
            "content_type": "binary/custom-server-binary-type",
            "content_encoding": "binary",
        }
        return response

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
            if self.request is None:
                self.process_request()


    def write(self):
        if self.request:
            if not self.request_created:
                self.create_response()
        self._write()


    def close(self):
        print("Closing connection to:", self.addr)
        try:
            self.selector.unregister(self.sock)
        except Exception as e:
            print("Error: selector.unregister() exeption for", f"{self.addr}: {repr(e)}")
        finally:
            self.sock = None #Delete reference to socket object for garbage collection


    def process_protoheader(self):
        hdrlen = 2 #Proto Header is 2 bytes long
        
        if len(self._recv_buffer) >= hdrlen: #If there have been more than 2 bytes received
            
            self._jsonheader_len = struct.unpack(">h", self._recv_buffer[:hdrlen])[0] #Unpack first two bytes (proto header) Big-Endian received data and store in _jsonheader_len
            
            self._recv_buffer = self._recv_buffer[hdrlen:] #Remove first two bytes from received data 


    def process_jsonheader(self):
        hdrlen = self._jsonheader_len #Utilize value from process_protoheader()
        
        if len(self._recv_buffer) >= hdrlen: #Check to see if we have received data that is at least amount specified by _jsonheader_len
 
            self.jsonheader = self._json_decode(self._recv_buffer[:hdrlen], "utf-8") #Unpack first-_jsonheader_len bytes from received data
            
            self._recv_buffer = self._recv_buffer[hdrlen:] #Remove json header from received data
            
            for reqhdr in ("byteorder", "content-length", "content-type", "content-encoding"): #Check to see that all necessary json data is present
                
                if reqhdr not in self.jsonheader: #Raise error if value missing
                    raise ValueError('Missing required header "[reqhdr}".')

    def process_request(self):
        print('process_request')
        content_len = self.jsonheader["content-length"] #Grab content_length from jsonheader
        if not len(self._recv_buffer) >= content_len: #Check to see if we have received data that is at least amount specified by content_length
            print('notenoughdata.error')
            return
        
        data = self._recv_buffer[:content_len] #Variable to store content data
        self._recv_buffer = self._recv_buffer[content_len:] #Remove content data from received data

        if self.jsonheader["content-type"] == "text/json": #Process Data and set selector event to Write mode
            print('json')
            encoding = self.jsonheader["content-encoding"]
            self.request = self._json_decode(data, encoding)
            print("received request", repr(self.request), "from", self.addr)
        else:
            self.request = data
            print(f'received {self.jsonheader["content-type"]} request from', self.addr)
        print(self.request)
        self._set_selector_events_mask("w")
        print('write')

    def create_response(self):
        print('create')
        if self.jsonheader["content-type"] == "text/json":
            response = self._create_response_json_content()
        else:
            response = self._create_response_binary_content()
        message = self._create_message(**response)
        self.response_created = True
        self._send_buffer += message

