# riverhog_storage_adapter_protocol.ReadReady

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-readready:2778027f28 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f6acfd0ad5"></a>
- <a id="s-353ecf5157"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-507f34c2cc"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-a9c9267975"></a>`name`: `ReadReady`
- <a id="s-35078adbd3"></a>`unit`: `export`

### Declared structure

- <a id="s-920d0c637a"></a>`kind`: `"class"`
- <a id="s-2f1dd9704d"></a>`signature`: `"\"(*, state: Literal['ready'] = 'ready', available_until: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=100)] = None) -> None\""`

#### Validated model schema

<a id="s-10ea5f1e41"></a>

- <a id="s-09bdf6c607"></a>`type`: `"object"`
- <a id="s-669c6687b4"></a>`additionalProperties`: `false`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-436877d32c"></a>`available_until` | no | anyOf=[(type="string"; maxLength=100; minLength=1); (type="null")]; default=null |  |
| <a id="s-6d94a26d0a"></a>`state` | no | type="string"; const="ready"; default="ready" |  |

## Maintained corroboration

### Related interface records

- [canonical_available_until](riverhog-storage-adapter-protocol-readready-canonical-available-until.md)

## Governing policies

- <a id="pa-670b65e44f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ReadReady`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8fc44485c0bbe79514b258e2b7ed3cadb78221bd3ccda422a71dc2d95bd62e31 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "available_until": {
          "anyOf": [
            {
              "maxLength": 100,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "state": {
          "const": "ready",
          "default": "ready",
          "type": "string"
        }
      },
      "type": "object"
    },
    "signature": "\"(*, state: Literal['ready'] = 'ready', available_until: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=100)] = None) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ReadReady",
  "unit": "export"
}
```

</details>
