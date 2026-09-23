# riverhog_provenance.FileStateObservationResult.assertion_body

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-filestateobservationr-362f7bf499:d7ac3132e1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e88067a783"></a>
- <a id="s-0028d6622d"></a>`distribution`: `riverhog-provenance`
- <a id="s-4c8b85287f"></a>`module`: `riverhog_provenance`
- <a id="s-3c7ad90cb7"></a>`name`: `assertion_body`
- <a id="s-64b8df507f"></a>`owner`: `riverhog_provenance.FileStateObservationResult`
- <a id="s-76f884ef35"></a>`unit`: `member`

### Declared structure

- <a id="s-1044c3016c"></a>`kind`: `"method"`
- <a id="s-c16a8c8207"></a>`signature`: `"\"(self, *, omit_object_ids: 'Sequence[str]' = ()) -> 'JsonObject'\""`

## Maintained corroboration

### Related interface records

- [FileStateObservationResult](riverhog-provenance-filestateobservationresult.md)

## Governing policies

- <a id="pa-e0ce3ba2cf"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.FileStateObservationResult.assertion_body`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 09cf4152898c08af42d1e8701ff15ab4fe576274b5c187bdbf30e58fd3fb0be6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, omit_object_ids: 'Sequence[str]' = ()) -> 'JsonObject'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "assertion_body",
  "owner": "riverhog_provenance.FileStateObservationResult",
  "unit": "member"
}
```

</details>
