# dubbo-ssl-python

Python client for [Apache Dubbo](https://dubbo.apache.org) RPC protocol. Implements the dubbo wire protocol (16-byte header with magic `0xdabb`) + Hessian2 serialization,enabling Python application to invoke Java Dubbo 2.x services directly. Support TCP,SSL/TLS connections,Zookeeper services discovery.
Base on https://github.com/apache/dubbo-python2 and https://github.com/huisongyang/dubbo-python3, support Python3.

## Features

- Full Hessian2 serialization/deserialization (primitives, objects, lists, maps, dates, null, circular references)
- TCP and SSL/TLS connections with custom CA certificate support
- Zookeeper-based services discovery with weighted load balancing
- Connection pooling with automatic heartbeat and reconnection
- Support `group` settings for Dubbo, Compatible with Dubbo 2.x

## Requirements

- Python3.8+
- [kazoo](https://kazoo.readthedocs.io/). --Zookeeper client (required only if using ZkRegister)

## Installation

```bash
pip install dubbo-ssl-python
```

Dependencies:

- `kazoo` -- Zookeeper client for service discovery

## Quick start

### Direct Connection (TCP)

Connect to a Dubbo service without Zookeeper:

```python
from dubbo_ssl.client import DubboClient
from dubbo_ssl.codec.encoder import Object

client = DubboClient(
    interface="com.example.service.UserService",
    group="default",
    version="1.0.0",
    dubbo_version="2.7.23",
    host="127.0.0.1:20880",
    verify=None              # Plain TCP,no SSL
)

request = Object("com.example.service.UserService.dto.UserRequest")
request["userId"] = "A000021"
request["type"] = "QUERY"

result = client.call(method="getUser",args=request)
print(request)
```

### SSL/TLS Connection

Connect to a Dubbo service over SSL with custom CA certificate verification:

```python
from dubbo_ssl.client import DubboClient

client = DubboClient(
    interface="com.example.service.UserService",
    group="default",
    version="1.0.0",
    dubbo_version="2.7.23",
    host="127.0.0.1:20880",
    verify="/path/to/DubboRootCA.cer"    # Custom CA certificate file
)

request = " Hello, SSL Provider."
result = client.call(method="getUser",args=request)
print(request)
```

The `verify` parameter supports three SSL models:

| Value               | Behavior                                                             |
| ------------------- | -------------------------------------------------------------------- |
| `None`              | Plain TCP,no SSL                                                     |
| `"/path/to/ca.crt"` | SSL with custom CA certificate verification, hostname check disabled |
| `False`             | SSL with no server certificate verification (insecure)               |

### Zookeeper Service discovery

Use Zookeeper for automatic service discovery and load balancing:

```python
from dubbo_ssl.client import DubboClient, ZkRegister

zk = ZkRegister(hosts="127.0.0.1:2181", application_name="MyAPP")
client = DubboClient(
    interface="com.example.service.UserService",
    group="default",
    version="1.0.0",
    dubbo_version="2.7.23",
    zk_register=zk
)

request = " Hello, SSL Provider."
result = client.call(method="getUser",args=request)
# When done
zk.close()
```

### Multiple Arguments

Pass multiple arguments to a method

```python
result = client.call(
    method="query",
    args=["keyword",10,5]
)
```

### Custom Context (Attachments)

Pass RPC attachment metadata ( key-value pairs sent alongside the request)

```python
result = client.call(
    method="getRequest",
    args=["keyword",10,5],
    context={"traceId":"x-123","tenantId":"T001"}
)
```

### Timeout

Set a request timeout in seconds.

```python
result = client.call(
method="getRequest",
args=["keyword"],
timeout=5
)
```

## Type Mapping

### Python -> Java (Request Encoding)

| Python Type                | Java Type          | Hessian2 Code        | Note                                             |
| -------------------------- | ------------------ | -------------------- | ------------------------------------------------ |
| `bool`                     | `boolean`          | `T`/ `F`             |                                                  |
| `int` (within int32 range) | `int`              | Compact `I` encoding | -2^31 ~ 2^31-1                                   |
| `int` (exceed int32)       | `long`             | `L` (8-byte)         |                                                  |
| `float`                    | `double`           | Compact `D` encoding | Includes 0.0/1.0 short forms                     |
| `str`                      | `java.lang.String` | Short `S` encoding   | UTF-8                                            |
| `Object`                   | Java class         | `C` + `O`            | Class definition + instance                      |
| `list`                     | Java array         | Typed list encoding  | Elements must be same type; empty list -> `null` |
| `None`                     | null               | `N`                  |                                                  |

### Java -> Python (Response Decoding)

| Java Type              | Python Type | Hessian2 Code                           |
| ---------------------- | ----------- | --------------------------------------- |
| `boolean`              | `boolean`   | `T`/ `F`                                |
| `int`                  | `int`       | Compact `I` / `I` (4-byte)              |
| `long`                 | `int`       | Compact / `L` (8-byte)                  |
| `double`               | `float`     | Compact `D` / `D` (8-byte IEEE 754)     |
| `java.lang.String`     | `str`       | Short / chunked `S` / `R`               |
| java object            | `dict`      | `C` + `O` (field names as keys)         |
| `java.math.BigDecimal` | `float`     | Auto-converted from `value` field       |
| `java.math.BigInteger` | `int`       | Auto-converted from `value` field       |
| List / array           | `list`      | Type / untyped, fixed / variable length |
| Map                    | `dict`      | `M` / `H` (untyped)                     |
| `java.util.Date`       | `str`       | ISO 8601 format string                  |
| `null`                 | `None`      | `N`                                     |
| Circular reference     | resolved    | `Q` (ref)                               |

## License

Apache License 2.0
