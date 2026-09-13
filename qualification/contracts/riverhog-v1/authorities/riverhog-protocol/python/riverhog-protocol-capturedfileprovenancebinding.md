# riverhog_protocol.CapturedFileProvenanceBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-capturedfileprovenancebinding:cd6e15239d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-039a76c3bb"></a>
| Field | Shape |
|---|---|
| <a id="s-9072e21659"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-40e5f62b70"></a>`distribution` | "riverhog-protocol" |
| <a id="s-dff7b28bfb"></a>`module` | "riverhog_protocol" |
| <a id="s-a66d13507a"></a>`name` | "CapturedFileProvenanceBinding" |
| <a id="s-9e88557392"></a>`unit` | "export" |

## Governing policies

- <a id="pa-51a36f9abe"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CapturedFileProvenanceBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e2595805ac64f1c0b32d70228ff62ba52a761b24a62bdb2ead033e4f11744df9 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "373a03b50758a99eed348b2b9a11439bdca4263639c942bbf223f46510326b64",
    "signature": "\"(*, journal_id: ProvenanceJournalId, current_state_id: ProvenanceStateId, status: Literal['captured']) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CapturedFileProvenanceBinding",
  "unit": "export"
}
```
