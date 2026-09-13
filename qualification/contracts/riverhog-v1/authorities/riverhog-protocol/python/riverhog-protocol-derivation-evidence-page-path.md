# riverhog_protocol.derivation_evidence_page_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-derivation-evidence-page-path:ca045dfc24 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a354e74ac0"></a>
| Field | Shape |
|---|---|
| <a id="s-8db60edd88"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-9c1c919119"></a>`distribution` | "riverhog-protocol" |
| <a id="s-3ae9482bdd"></a>`module` | "riverhog_protocol" |
| <a id="s-71c9c213f2"></a>`name` | "derivation_evidence_page_path" |
| <a id="s-ef91c4d07a"></a>`unit` | "export" |

## Governing policies

- <a id="pa-2315de6d32"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.derivation_evidence_page_path`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 061b9df3068284ab10019940caba5dccd564bd1ffa7a68ab74773307629c85c9 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "'(kind: \"Literal[\\'dispositions\\', \\'output-edges\\']\", start_ordinal: \\'int\\') -> \\'str\\''"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "derivation_evidence_page_path",
  "unit": "export"
}
```
