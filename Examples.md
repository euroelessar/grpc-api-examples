# Definition For Each Set of API

* Current: Existing gRPC Python API
* Option 1: Async API similar to existing gRPC Python API
* Option 2: Brand new Async API
* Option 3: Brand new Async API with explicit context and strong typing
* Ideal: The most idealistic `asyncio` approach

## Motivation
* Asynchronous processing perfectly fits IO-intensive gRPC use cases;
* Resolve a long-living design flaw of thread exhaustion problem;
* Performance is much better than the multi-threading model;
* Provide a clean flow for passing metadata across RPC boundaries.

# Client Side Examples
## Unary-Unary Call

### Current API

```Python
with grpc.insecure_channel('localhost:50051') as channel:
    stub = helloworld_pb2_grpc.GreeterStub(channel)
    response = stub.Hi(...)
```

### Option 1 / Option 2

```Python
async with grpc.insecure_channel('localhost:50051') as channel:
    stub = helloworld_pb2_grpc.GreeterStub(channel)
    response = await stub.Hi(...)
```

### Option 3

```Python
ctx = grpc.Context() \
    .with_timeout_secs(5.0) \
    .append_outgoing_metadata('key', 'value')
# or: ctx.with_deadline(time.time() + 5.0)

async with grpc.insecure_channel('localhost:50051') as channel:
    stub = helloworld_pb2_grpc.AsyncGreeterClient(channel)
    response = await stub.Hi(ctx, helloworld_pb2.GreetRequest(...))
```

### Dynamic

```Python
helloworld_protos = grpc.proto('helloworld.proto')
response = await helloworld_protos.Hi('localhost:50051', request)
```

# Stream-Stream Call

### Current API

```Python
class RequestIterator:

    def __init__(self):
        self._queue = queue.Queue()

    def send(self, message):
        self._queue.put(message)

    def __iter__(self):
        return self

    def __next__(self):
        return self._queue.get(block=True)

request_iterator = RequestIterator()
response_iterator = stub.StreamingHi(request_iterator)

# In sending thread
request_iterator.send(proto_message)

# In receiving thread
for response in response_iterator:
    process(response)
```

A special advanced usage that can shrink the LOC:

```Python
request_queue = queue.Queue()
response_iterator = stub.StreamingHi(iter(request_queue.get, None))

# In sending thread
request_iterator.send(proto_message)

# In receiving thread
for response in response_iterator:
    process(response)
```

### Option 1

```Python
class RequestIterator:

    def __init__(self):
        self._queue = asyncio.Queue()

    def send(self, message):
        self._queue.put(message)

    def __iter__(self):
        return self

    async def __anext__(self):
        return await self._queue.get()

request_iterator = RequestIterator()
response_iterator = stub.StreamingHi(request_iterator)

# In sending thread
await request_iterator.send(proto_message)

# In receiving thread
async for response in response_iterator:
    process(response)
```

### Option 2

```Python
call = stub.StreamingHi()

# In sending thread
await call.send(proto_message)

# In receiving thread
try:
    response = await call.receive()
    process(response)
except grpc.EOF:
    pass
```

### Option 3

Use `Optional[helloworld_pb.Message]` as `receive` return type, it indicates
that it is expected behavior for the stream to end, and forces a user to write
code to handle this non-special condition.

```Python
stream = stub.StreamingHi(ctx)

# In sending thread
await call.send(proto_message)

# In receiving thread
while True:
    response = await call.receive()
    if response is None:
        return
    process(response)
```

### Dynamic

```Python
helloworld_protos = grpc.proto('helloworld.proto')
call = await helloworld_protos.StreamingHi('localhost:50051')

# In sending thread
await call.send(proto_message)

# In receiving thread
try:
    while True:
        response = await call.receive()
        process(response)
except grpc.EOF:
    pass
```

# Server Side Example

## Server Creation

### Current API

```Python
server = grpc.server(ThreadPoolExecutor(max_workers=10))
server.add_insecure_port(':50051')
helloworld_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
server.start()
server.wait_for_termination()
```

### Option 1 / Option 2 / Ideal

```Python
server = grpc.server()
server.add_insecure_port(':50051')
helloworld_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
server.start()
await server.wait_for_termination()
```

### Option 3

```Python
server = grpc.server()
server.add_insecure_port(':50051')
helloworld_pb2_grpc.add_AsyncGreeterServicer_to_server(Greeter(), server)
# or helloworld_pb2_async_grpc.add_GreeterServicer_to_server(Greeter(), server)
server.start()
await server.wait_for_termination()
```

## Unary-Unary Handler

### Current API

```Python
class Greeter(helloworld_pb2_grpc.GreeterServicer):
    def Hi(self, request, context):
        return ...
```

### Option 1 / Option 2 / Ideal

```Python
class Greeter(helloworld_pb2_grpc.GreeterServicer):
    async def Hi(self, request, context):
        response = await some_io_operation
        return response
```

## Option 3

```Python
class Greeter(object):
    async def Hi(self,
                 ctx: grpc.Context,
                 request: helloworld_pb2.HiRequest,
        ) -> helloworld_pb2.HiResponse:
        response = await some_io_operation
        return response
```

## Stream-Stream Handler

### Current API

```Python
class ResponseIterator:

    def __init__(self):
        self._queue = queue.Queue()

    def send(self, message):
        self._queue.put(message)

    def __iter__(self):
        return self

    def __next__(self):
        return self._queue.get()

def streaming_hi_worker(request_iterator, response_iterator):
    for request in request_iterator:
        if request.needs_respond:
            response_iterator.send(response)

class Greeter(helloworld_pb2_grpc.GreeterServicer):
    def StreamingHi(self, request_iterator, context):
        response_iterator = ResponseIterator()
        background_thread = threading.Thread(target=streaming_hi_worker)
        background_thread.daemon = True
        background_thread.start()
        return response_iterator
```

The most simple case of streaming handler is the responses one-to-one mapped to
the requests.

```Python
class Greeter(helloworld_pb2_grpc.GreeterServicer):
    def StreamingHi(self, request_iterator, context):
        for request in request_iterator:
            yield response
```

### Option 1

```Python
class ResponseIterator:

    def __init__(self):
        self._queue = asyncio.Queue()

    def send(self, message):
        self._queue.put(message)

    def __iter__(self):
        return self

    async def __anext__(self):
        return await self._queue.get()

def streaming_hi_worker(request_iterator, response_iterator):
    async for request in request_iterator:
        if request.needs_respond:
            await response_iterator.send(response)

class Greeter(helloworld_pb2_grpc.GreeterServicer):
    def StreamingHi(self, request_iterator, context):
        response_iterator = ResponseIterator()
        background_thread = threading.Thread(target=streaming_hi_worker)
        background_thread.daemon = True
        background_thread.start()
        return response_iterator
```

```Python
class Greeter(helloworld_pb2_grpc.GreeterServicer):
    async def StreamingHi(self, request_iterator, context):
        async for request in request_iterator:
            yield response
```

### Option 2 / Ideal

```Python
class Greeter(helloworld_pb2_grpc.GreeterServicer):
    def StreamingHi(self, context):
        while True:
            request = await context.receive()
            if request.needs_respond:
                await context.send(response)
```

## Option 3

```Python
class Greeter(object):
    async def StreamingHi(self, stream: helloworld_grpc_pb2.HiStream) -> None:
        while True:
            request = await stream.receive()
            if request.needs_respond:
                await stream.send(response)
```

# Generated File

## Current / Option 1

Keep the generated file untouched. Only swap the underlying implementation.

```Python
import helloworld_pb2
import helloworld_pb2_grpc
channel = grpc.insecure_channel('localhost:50051'):
stub = helloworld_pb2_grpc.GreeterStub(channel)
...
```

## Option 2

The new generated file is clean, and potentially enables us to add breaking
changes.
Allows using the same channel/server across both sync and async stubs.

```Python
import helloworld_pb2_grpc_async
channel = grpc.insecure_channel('localhost:50051'):
stub = helloworld_pb2_grpc_async.GreeterStub(channel)
```

## Option 3

Generate async stubs with `Async` prefix alonside synchronous ones.
Allows using the same channel/server across both sync and async stubs, but can
lead to namespace conflict.

```Python
import helloworld_pb2
import helloworld_pb2_grpc
channel = grpc.insecure_channel('localhost:50051'):
stub = helloworld_pb2_grpc.AsyncGreeterStub(channel)
```

# Typing
### Option 1
Status quo, concrete abstract classes.

```Python
class GreeterServicer(object):
    async def Hi(self,
                 ctx: grpc.Context,
                 request: helloworld_pb2.HiRequest,
        ) -> helloworld_pb2.HiResponse:
        raise NotImplementedError('Method not implemented!')

    async def StreamingHi(self, stream: grpc.Stream) -> None:
        msg = await stream.receive()
        reveal_type(msg)  # observed type: Any
        raise NotImplementedError('Method not implemented!')
```

### Option 2
Use PEP 484 protocols. Abstracts implementation away at ≈zero runtime cost
and simplifies ability to mock implementation for tests.

Individual stream objects are strongly typed, which increases code
maintainability and reduces chance of runtime bugs.

```Python
class HiStream(typing.Protocol):
    def context(self) -> grpc.Context:
        pass

    async def receive(self) -> HiResponse:
        pass

    async def send(self, req: HiRequest) -> None:
        pass

    async def close_send(self) -> None:
        pass


class GreeterServicer(typing.Protocol):
    async def Hi(self,
                 ctx: grpc.Context,
                 request: helloworld_pb2.HiRequest,
        ) -> helloworld_pb2.HiResponse:
        pass

    async def StreamingHi(self, stream: helloworld_grpc_pb2.HiStream) -> None:
        pass
```