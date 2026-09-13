# riverhog_protocol.TransformCapabilityCreateDocument.validate_capability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-transformcapabilitycrea-519871881f:cc3895872d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-207f689cc8"></a>
| Field | Shape |
|---|---|
| <a id="s-19a8c39136"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-40d3a7aea2"></a>`distribution` | "riverhog-protocol" |
| <a id="s-a5b91b6f27"></a>`module` | "riverhog_protocol" |
| <a id="s-a4112facf7"></a>`name` | "validate_capability" |
| <a id="s-e26381c781"></a>`owner` | "riverhog_protocol.TransformCapabilityCreateDocument" |
| <a id="s-2bad4ca673"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.TransformCapabilityCreateDocument](riverhog-protocol-transformcapabilitycreatedocument.md)

## Governing policies

- <a id="pa-cc7450f586"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.TransformCapabilityCreateDocument.validate_capability`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 00a726d6903c6c61c2ac267c6af2104d4a8d8d261a865367c008344154084b0b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_capability",
  "owner": "riverhog_protocol.TransformCapabilityCreateDocument",
  "unit": "member"
}
```
