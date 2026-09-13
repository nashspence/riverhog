# riverhog_protocol.ProcessingOutcomeBindingDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingoutcomebindingdocument:c6ec8a4c02 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-036f962df3"></a>
| Field | Shape |
|---|---|
| <a id="s-6fecdc83cc"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-70a71c9379"></a>`distribution` | "riverhog-protocol" |
| <a id="s-6609b97483"></a>`module` | "riverhog_protocol" |
| <a id="s-c868405ca9"></a>`name` | "ProcessingOutcomeBindingDocument" |
| <a id="s-2f9ad98ba9"></a>`unit` | "export" |

## Governing policies

- <a id="pa-f054bd7fb0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingOutcomeBindingDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a89cf1abf68d3bfb232579394088d1af3356dcea5785ad89937696c51a6c1f48 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "53b400e47c4282f39ca9413347f8cd65db26d06e70dab9ef036d89feaa731635",
    "signature": "\"(*, claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], fence: Annotated[int, Ge(ge=1)], outcome_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingOutcomeBindingDocument",
  "unit": "export"
}
```
