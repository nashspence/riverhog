# riverhog_protocol.verify_derivation_evidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-verify-derivation-evidence:cbeb3de4ed -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6c6107de8d"></a>
| Field | Shape |
|---|---|
| <a id="s-4aabb0d234"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-0fdd2eae40"></a>`distribution` | "riverhog-protocol" |
| <a id="s-31ac96d3df"></a>`module` | "riverhog_protocol" |
| <a id="s-325ec7290b"></a>`name` | "verify_derivation_evidence" |
| <a id="s-287fd4b817"></a>`unit` | "export" |

## Governing policies

- <a id="pa-4264a3e21e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.verify_derivation_evidence`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 71444c7a8d9a26c03457f4ccf580379e0e50bac1a37b7fb1e092fb20e478998a -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(disposition_pages: 'Iterable[bytes]', output_pages: 'Iterable[bytes]', *, expected: 'ArtifactDispositionSetIdentity') -> 'ArtifactDispositionSetIdentity'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "verify_derivation_evidence",
  "unit": "export"
}
```
