# riverhog_provenance.ObservationResult.assertion_body

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-observationresult-assertion-body:abceed1e63 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7dc792a3e3"></a>
- <a id="s-9c8b4f7da6"></a>`distribution`: `riverhog-provenance`
- <a id="s-e6b318aa1d"></a>`module`: `riverhog_provenance`
- <a id="s-9b81b8ba95"></a>`name`: `assertion_body`
- <a id="s-b5ec0302ed"></a>`owner`: `riverhog_provenance.ObservationResult`
- <a id="s-4441af1383"></a>`unit`: `member`

### Declared structure

- <a id="s-ab6765baf1"></a>`kind`: `"method"`
- <a id="s-64f6547d69"></a>`signature`: `"\"(self, *, omit_object_ids: 'Sequence[str]' = ()) -> 'JsonObject'\""`

## Maintained corroboration

### Related interface records

- [ObservationResult](riverhog-provenance-observationresult.md)

## Governing policies

- <a id="pa-41d551cd99"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.ObservationResult.assertion_body`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: caf159127ecb214bf213d880b6609e9c00bce5a7a5b9f3f2cafd8390ccdb8a5c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, omit_object_ids: 'Sequence[str]' = ()) -> 'JsonObject'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "assertion_body",
  "owner": "riverhog_provenance.ObservationResult",
  "unit": "member"
}
```
