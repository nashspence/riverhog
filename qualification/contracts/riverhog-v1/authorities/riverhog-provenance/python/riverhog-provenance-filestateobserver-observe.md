# riverhog_provenance.FileStateObserver.observe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-filestateobserver-observe:b81e393a9b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4d302e54e5"></a>
- <a id="s-5496661b65"></a>`distribution`: `riverhog-provenance`
- <a id="s-c74b41273e"></a>`module`: `riverhog_provenance`
- <a id="s-2c0f756ccb"></a>`name`: `observe`
- <a id="s-a0cf395cb7"></a>`owner`: `riverhog_provenance.FileStateObserver`
- <a id="s-441d9541e3"></a>`unit`: `member`

### Declared structure

- <a id="s-0f5e979f0a"></a>`kind`: `"method"`
- <a id="s-1655d1a8aa"></a>`signature`: `"\"(self, request: 'ObservationRequest') -> 'ObservationResult'\""`

## Maintained corroboration

### Related interface records

- [FileStateObserver](riverhog-provenance-filestateobserver.md)

## Governing policies

- <a id="pa-feff087902"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.FileStateObserver.observe`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 445cb111659a9d3411c45e72fa20598714a4f8ea48e828fb7e39f0de6d3cdaa1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ObservationRequest') -> 'ObservationResult'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "observe",
  "owner": "riverhog_provenance.FileStateObserver",
  "unit": "member"
}
```

</details>
