# riverhog_protocol.ImmutableFileIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-immutablefileidentitydocument:d5464de65b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f1bd22a3d6"></a>
| Field | Shape |
|---|---|
| <a id="s-3ab3990bfb"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-99a9e71ff8"></a>`distribution` | "riverhog-protocol" |
| <a id="s-aeced36ffc"></a>`module` | "riverhog_protocol" |
| <a id="s-d58d184434"></a>`name` | "ImmutableFileIdentityDocument" |
| <a id="s-c4a485c480"></a>`unit` | "export" |

## Governing policies

- <a id="pa-8aa7427881"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ImmutableFileIdentityDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 507d3de029df8e7e78b8454326edd8d53f7cefa9c9ec673ad9ba9465d0470eb4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "b70f55d90a8f7b3a8506e96e423fbbf6705b9d55d919d9918254e4a3da6188b1",
    "signature": "\"(*, path: CanonicalRelPath, bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ImmutableFileIdentityDocument",
  "unit": "export"
}
```
