# stove0_protocol.ControllerEvidencePayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-controllerevidencepayload:43a21c32f1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bfcef07647"></a>
| Field | Shape |
|---|---|
| <a id="s-7b75a198b7"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-c9085f3772"></a>`distribution` | "stove0-protocol" |
| <a id="s-181aead80a"></a>`module` | "stove0_protocol" |
| <a id="s-b5c8927554"></a>`name` | "ControllerEvidencePayload" |
| <a id="s-a65312c23a"></a>`unit` | "export" |

## Governing policies

- <a id="pa-1450a22284"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ControllerEvidencePayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8357c4e6997bd15c44805f2a977de3098204cf9fb95839e47edbc35e54327d68 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "3c3aad377f9e1e0b63e741284adbda66871bea711a3f40763d750f55966ed91b",
    "signature": "\"(*, format: Literal['stove0-controller-evidence/v1'] = 'stove0-controller-evidence/v1', execution_envelope: stove0_protocol.models.ExecutionEnvelope) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ControllerEvidencePayload",
  "unit": "export"
}
```
