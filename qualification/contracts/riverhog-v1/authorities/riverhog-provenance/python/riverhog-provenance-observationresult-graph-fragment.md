# riverhog_provenance.ObservationResult.graph_fragment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-observationresult-graph-fragment:3feea39893 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2a8cba2e24"></a>
- <a id="s-b57591b1b3"></a>`distribution`: `riverhog-provenance`
- <a id="s-5b0d75c2b7"></a>`module`: `riverhog_provenance`
- <a id="s-164bee9016"></a>`name`: `graph_fragment`
- <a id="s-dc6c30db3a"></a>`owner`: `riverhog_provenance.ObservationResult`
- <a id="s-bd43f9fc62"></a>`unit`: `member`

### Declared structure

- <a id="s-97e130047e"></a>`kind`: `"method"`
- <a id="s-9643a6acde"></a>`signature`: `"\"(self, *, omit_object_ids: 'Sequence[str]' = ()) -> 'JsonObject'\""`

## Maintained corroboration

### Related interface records

- [riverhog_provenance.ObservationResult](riverhog-provenance-observationresult.md)

## Governing policies

- <a id="pa-a415a39266"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.ObservationResult.graph_fragment`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 71a7c08b2659a64601774da931ae13309280ef664daf9dd37bf9570cd6bf2846 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, omit_object_ids: 'Sequence[str]' = ()) -> 'JsonObject'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "graph_fragment",
  "owner": "riverhog_provenance.ObservationResult",
  "unit": "member"
}
```
