# riverhog_client.ProducerStream

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-producerstream:2c35732506 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ce5bdbb8fe"></a>
- <a id="s-d42a1099e9"></a>`distribution`: `riverhog-client`
- <a id="s-416ea2e995"></a>`module`: `riverhog_client`
- <a id="s-61130c16a2"></a>`name`: `ProducerStream`
- <a id="s-7cc20ce22c"></a>`unit`: `export`

### Declared structure

- <a id="s-73c5cd69c2"></a>`kind`: `"class"`
- <a id="s-558926d1c9"></a>`signature`: `"\"(path: 'str', bytes: 'int', sha256: 'str', read_range: 'RangeReader', provenance: 'Mapping[str, object] \| None' = None) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-b300225bde"></a>`path` | `'str'` | `required` |
| <a id="s-ab612a4a85"></a>`bytes` | `'int'` | `required` |
| <a id="s-b178f100b1"></a>`sha256` | `'str'` | `required` |
| <a id="s-c9d1a9c9ee"></a>`read_range` | `'RangeReader'` | `required` |
| <a id="s-fb6869170f"></a>`provenance` | `'Mapping[str, object] \| None'` | `None` |

## Governing policies

- <a id="pa-0c9b6fe5bc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ProducerStream`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8da596d6eb630ccad537f9dadffe2351bb50b8286b7c41951bf917582d614cb4 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "path",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "bytes",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "read_range",
        "type": "'RangeReader'"
      },
      {
        "default": "None",
        "name": "provenance",
        "type": "'Mapping[str, object] | None'"
      }
    ],
    "kind": "class",
    "signature": "\"(path: 'str', bytes: 'int', sha256: 'str', read_range: 'RangeReader', provenance: 'Mapping[str, object] | None' = None) -> None\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "ProducerStream",
  "unit": "export"
}
```

</details>
