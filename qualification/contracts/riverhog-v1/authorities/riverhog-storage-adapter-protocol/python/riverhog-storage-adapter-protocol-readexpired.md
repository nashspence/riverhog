# riverhog_storage_adapter_protocol.ReadExpired

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-readexpired:98d4f677bd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6b9cbbcc9f"></a>
- <a id="s-c5489728a3"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-61ef9f8be6"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-0d9a6017ff"></a>`name`: `ReadExpired`
- <a id="s-ff88e97bd4"></a>`unit`: `export`

### Declared structure

- <a id="s-7bdec347b7"></a>`kind`: `"class"`
- <a id="s-4139e553fa"></a>`signature`: `"\"(*, state: Literal['expired'] = 'expired') -> None\""`

#### Validated model schema

<a id="s-9d8f3b8f83"></a>

- <a id="s-4b7db32ec7"></a>`type`: `"object"`
- <a id="s-e53a6fe610"></a>`additionalProperties`: `false`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-efdd766f55"></a>`state` | no | type="string"; const="expired"; default="expired" |  |

## Governing policies

- <a id="pa-12a93238ba"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ReadExpired`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 872a514885f61badd66cba8b2cc2337f85a04bf807d5950445e3dff33f830a42 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "state": {
          "const": "expired",
          "default": "expired",
          "type": "string"
        }
      },
      "type": "object"
    },
    "signature": "\"(*, state: Literal['expired'] = 'expired') -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ReadExpired",
  "unit": "export"
}
```

</details>
