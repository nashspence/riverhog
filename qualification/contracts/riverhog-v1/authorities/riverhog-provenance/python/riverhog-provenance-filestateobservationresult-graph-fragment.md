# riverhog_provenance.FileStateObservationResult.graph_fragment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-filestateobservationr-ce4b501d34:cdbc7a51ca -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-feb03c46d1"></a>
- <a id="s-8fe61cd736"></a>`distribution`: `riverhog-provenance`
- <a id="s-a2b700eb41"></a>`module`: `riverhog_provenance`
- <a id="s-84d5d4caaf"></a>`name`: `graph_fragment`
- <a id="s-ecefa51073"></a>`owner`: `riverhog_provenance.FileStateObservationResult`
- <a id="s-7dfcb3fcf8"></a>`unit`: `member`

### Declared structure

- <a id="s-5b979896a6"></a>`kind`: `"method"`
- <a id="s-19849ab0f7"></a>`signature`: `"\"(self, *, omit_object_ids: 'Sequence[str]' = ()) -> 'JsonObject'\""`

## Maintained corroboration

### Related interface records

- [FileStateObservationResult](riverhog-provenance-filestateobservationresult.md)

## Governing policies

- <a id="pa-6d2924c707"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.FileStateObservationResult.graph_fragment`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2289d43d2eec88a297e004f13037547c22a4a6e3b13678355728d280c0961b12 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, omit_object_ids: 'Sequence[str]' = ()) -> 'JsonObject'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "graph_fragment",
  "owner": "riverhog_provenance.FileStateObservationResult",
  "unit": "member"
}
```

</details>
