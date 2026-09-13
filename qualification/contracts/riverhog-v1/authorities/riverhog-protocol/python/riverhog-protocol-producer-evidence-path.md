# riverhog_protocol.PRODUCER_EVIDENCE_PATH

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-producer-evidence-path:5ea3d9c104 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-177432427e"></a>
| Field | Shape |
|---|---|
| <a id="s-2f6d0f6c20"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-9fa6755289"></a>`distribution` | "riverhog-protocol" |
| <a id="s-f6e57340d6"></a>`module` | "riverhog_protocol" |
| <a id="s-3fec6b9bdc"></a>`name` | "PRODUCER_EVIDENCE_PATH" |
| <a id="s-e5ffa90d92"></a>`unit` | "export" |

## Governing policies

- <a id="pa-f59e8cd0ee"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.PRODUCER_EVIDENCE_PATH`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3a2de74d396cfe4edbbb6663a8e04a8f9c2c15368793647327b0d19b02bfa6f0 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog/producer-evidence.json"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "PRODUCER_EVIDENCE_PATH",
  "unit": "export"
}
```
