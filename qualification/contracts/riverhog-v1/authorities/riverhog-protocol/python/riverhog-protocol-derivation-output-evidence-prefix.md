# riverhog_protocol.DERIVATION_OUTPUT_EVIDENCE_PREFIX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-derivation-output-evidence-prefix:e837992d73 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b47db3c124"></a>
| Field | Shape |
|---|---|
| <a id="s-ca16f99509"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-48b3e351d6"></a>`distribution` | "riverhog-protocol" |
| <a id="s-f66dbde366"></a>`module` | "riverhog_protocol" |
| <a id="s-484ef859c1"></a>`name` | "DERIVATION_OUTPUT_EVIDENCE_PREFIX" |
| <a id="s-76a34d1e58"></a>`unit` | "export" |

## Governing policies

- <a id="pa-0b069c8818"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.DERIVATION_OUTPUT_EVIDENCE_PREFIX`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ed10659cd43ad44b13e8aad3b68260e62072f7dd99d3b1fd83a7aa8c127ddbea -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog/derivation/output-edges"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "DERIVATION_OUTPUT_EVIDENCE_PREFIX",
  "unit": "export"
}
```
