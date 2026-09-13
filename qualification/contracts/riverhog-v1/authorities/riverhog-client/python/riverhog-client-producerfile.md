# riverhog_client.ProducerFile

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-producerfile:a5dabc065e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3bc5840053"></a>
| Field | Shape |
|---|---|
| <a id="s-3fdc097b1f"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-9cb68b0288"></a>`distribution` | "riverhog-client" |
| <a id="s-5e9e9dc26e"></a>`module` | "riverhog_client" |
| <a id="s-dca662952e"></a>`name` | "ProducerFile" |
| <a id="s-702821aa4a"></a>`unit` | "export" |

## Governing policies

- <a id="pa-fb92c0fa4e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ProducerFile`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d077d44997cff6438f640f913717fe9a1c3891c03cb8ac0fcdacc884e20a5b13 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "source",
        "type": "'Path'"
      },
      {
        "default": "required",
        "name": "path",
        "type": "'str'"
      },
      {
        "default": "None",
        "name": "provenance",
        "type": "'Mapping[str, object] | None'"
      }
    ],
    "kind": "class",
    "signature": "\"(source: 'Path', path: 'str', provenance: 'Mapping[str, object] | None' = None) -> None\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "ProducerFile",
  "unit": "export"
}
```
