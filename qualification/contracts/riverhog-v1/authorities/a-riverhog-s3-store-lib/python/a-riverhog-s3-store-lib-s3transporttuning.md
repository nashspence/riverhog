# a_riverhog_s3_store_lib.S3TransportTuning

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-s3-store-lib:a-riverhog-s3-store-lib-s3transporttuning:ed74203847 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-s3-store-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-760d702bba"></a>
- <a id="s-e2ce39b50d"></a>`distribution`: `a-riverhog-s3-store-lib`
- <a id="s-9c5a359f6e"></a>`module`: `a_riverhog_s3_store_lib`
- <a id="s-b8d6fa2c91"></a>`name`: `S3TransportTuning`
- <a id="s-e3207dfa32"></a>`unit`: `export`

### Declared structure

- <a id="s-05cbb3110f"></a>`kind`: `"class"`
- <a id="s-b783a2883e"></a>`signature`: `"'(max_pool_connections: \\'int\\' = 32, connect_timeout_seconds: \\'float\\' = 10.0, read_timeout_seconds: \\'float\\' = 300.0, max_attempts: \\'int\\' = 8, retry_mode: \"Literal[\\'standard\\', \\'adaptive\\']\" = \\'standard\\', tcp_keepalive: \\'bool\\' = True) -> None'"`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-2a28e2cbe4"></a>`max_pool_connections` | `'int'` | `32` |
| <a id="s-4f91bf0884"></a>`connect_timeout_seconds` | `'float'` | `10.0` |
| <a id="s-1c9c0b219b"></a>`read_timeout_seconds` | `'float'` | `300.0` |
| <a id="s-402c12f6b8"></a>`max_attempts` | `'int'` | `8` |
| <a id="s-09c5702a41"></a>`retry_mode` | `"Literal['standard', 'adaptive']"` | `'standard'` |
| <a id="s-a1a11166cc"></a>`tcp_keepalive` | `'bool'` | `True` |

## Governing policies

- <a id="pa-6589cbc2ad"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-s3-store-lib:a_riverhog_s3_store_lib](../../../evidence/sources/authorities.md#src-231d0d4373) — [some-implementations/riverhog/storage/s3-support/src/a\_riverhog\_s3\_store\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/s3-support/src/a_riverhog_s3_store_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_s3_store_lib.S3TransportTuning`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4656cd318cc9a0242fb3f8a4fef02435410a801baf75295061b3fc69b45fc55d -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "32",
        "name": "max_pool_connections",
        "type": "'int'"
      },
      {
        "default": "10.0",
        "name": "connect_timeout_seconds",
        "type": "'float'"
      },
      {
        "default": "300.0",
        "name": "read_timeout_seconds",
        "type": "'float'"
      },
      {
        "default": "8",
        "name": "max_attempts",
        "type": "'int'"
      },
      {
        "default": "'standard'",
        "name": "retry_mode",
        "type": "\"Literal['standard', 'adaptive']\""
      },
      {
        "default": "True",
        "name": "tcp_keepalive",
        "type": "'bool'"
      }
    ],
    "kind": "class",
    "signature": "'(max_pool_connections: \\'int\\' = 32, connect_timeout_seconds: \\'float\\' = 10.0, read_timeout_seconds: \\'float\\' = 300.0, max_attempts: \\'int\\' = 8, retry_mode: \"Literal[\\'standard\\', \\'adaptive\\']\" = \\'standard\\', tcp_keepalive: \\'bool\\' = True) -> None'"
  },
  "distribution": "a-riverhog-s3-store-lib",
  "module": "a_riverhog_s3_store_lib",
  "name": "S3TransportTuning",
  "unit": "export"
}
```

</details>
