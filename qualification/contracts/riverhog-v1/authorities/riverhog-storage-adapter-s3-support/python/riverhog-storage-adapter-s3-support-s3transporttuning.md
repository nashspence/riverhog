# riverhog_storage_adapter_s3_support.S3TransportTuning

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-s3tra-f5389b1b1d:29f85b4616 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-s3-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-831b825a2e"></a>
- <a id="s-16c3e40039"></a>`distribution`: `riverhog-storage-adapter-s3-support`
- <a id="s-df11edf9c1"></a>`module`: `riverhog_storage_adapter_s3_support`
- <a id="s-157b783649"></a>`name`: `S3TransportTuning`
- <a id="s-77d36ab0af"></a>`unit`: `export`

### Declared structure

- <a id="s-40958c2a9d"></a>`kind`: `"class"`
- <a id="s-96b54c041f"></a>`signature`: `"'(max_pool_connections: \\'int\\' = 32, connect_timeout_seconds: \\'float\\' = 10.0, read_timeout_seconds: \\'float\\' = 300.0, max_attempts: \\'int\\' = 8, retry_mode: \"Literal[\\'standard\\', \\'adaptive\\']\" = \\'standard\\', tcp_keepalive: \\'bool\\' = True) -> None'"`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-906b59ca62"></a>`max_pool_connections` | `'int'` | `32` |
| <a id="s-585a3a7c55"></a>`connect_timeout_seconds` | `'float'` | `10.0` |
| <a id="s-a9182458e0"></a>`read_timeout_seconds` | `'float'` | `300.0` |
| <a id="s-1358dc5058"></a>`max_attempts` | `'int'` | `8` |
| <a id="s-3a2a404f94"></a>`retry_mode` | `"Literal['standard', 'adaptive']"` | `'standard'` |
| <a id="s-3145cd5b7e"></a>`tcp_keepalive` | `'bool'` | `True` |

## Governing policies

- <a id="pa-042f3c143a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../../../evidence/sources.md#src-aa14de5031) — `reference/riverhog/storage/s3-support/src/riverhog_storage_adapter_s3_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_s3_support.S3TransportTuning`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6374edd292c54b5af3fc2584030958bec5967c798483abab3b6b01497fcfeaa3 -->

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
  "distribution": "riverhog-storage-adapter-s3-support",
  "module": "riverhog_storage_adapter_s3_support",
  "name": "S3TransportTuning",
  "unit": "export"
}
```

</details>
