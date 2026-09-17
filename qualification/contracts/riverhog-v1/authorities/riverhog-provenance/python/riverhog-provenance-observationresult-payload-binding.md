# riverhog_provenance.ObservationResult.payload_binding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-observationresult-pay-97112a1544:f868eb9e66 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7d15c9e149"></a>
- <a id="s-bd43bbda14"></a>`distribution`: `riverhog-provenance`
- <a id="s-2695f09207"></a>`module`: `riverhog_provenance`
- <a id="s-faed3d56a1"></a>`name`: `payload_binding`
- <a id="s-1c8c0420b1"></a>`owner`: `riverhog_provenance.ObservationResult`
- <a id="s-fa290a2f0a"></a>`unit`: `member`

### Declared structure

- <a id="s-4bc61c2b3f"></a>`kind`: `"property"`
- <a id="s-b0e2215f29"></a>`signature`: `"\"(self) -> 'JsonObject \| None'\""`

## Maintained corroboration

### Related interface records

- [ObservationResult](riverhog-provenance-observationresult.md)

## Governing policies

- <a id="pa-3b8923f57c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.ObservationResult.payload_binding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9106fce2cf86494bb46f3e066edd37c081d0549dbe0090e487aab6019007d68d -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'JsonObject | None'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "payload_binding",
  "owner": "riverhog_provenance.ObservationResult",
  "unit": "member"
}
```

</details>
