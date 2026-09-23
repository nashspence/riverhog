# riverhog_provenance.FileStateObservationResult.payload_binding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-filestateobservationr-a059d2a128:7f61a29ff1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8483abce41"></a>
- <a id="s-db92a8e2aa"></a>`distribution`: `riverhog-provenance`
- <a id="s-dbc8d042bd"></a>`module`: `riverhog_provenance`
- <a id="s-8b6e550ac7"></a>`name`: `payload_binding`
- <a id="s-8f3240b275"></a>`owner`: `riverhog_provenance.FileStateObservationResult`
- <a id="s-7949a92fdb"></a>`unit`: `member`

### Declared structure

- <a id="s-3110f6abc4"></a>`kind`: `"property"`
- <a id="s-10cdbf2e64"></a>`signature`: `"\"(self) -> 'JsonObject \| None'\""`

## Maintained corroboration

### Related interface records

- [FileStateObservationResult](riverhog-provenance-filestateobservationresult.md)

## Governing policies

- <a id="pa-c835f13510"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.FileStateObservationResult.payload_binding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0c95bcb941b1b5332bdff463d6fb987af8fbb9e0ef54a4c8aac88e7b0de29c99 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'JsonObject | None'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "payload_binding",
  "owner": "riverhog_provenance.FileStateObservationResult",
  "unit": "member"
}
```

</details>
