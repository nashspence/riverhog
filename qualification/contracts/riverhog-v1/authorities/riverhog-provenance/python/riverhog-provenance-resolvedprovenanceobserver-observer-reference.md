# riverhog_provenance.ResolvedProvenanceObserver.observer_reference

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-resolvedprovenanceobs-10cea0a3ad:32daa04e3f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-429952c3ce"></a>
- <a id="s-251212f9f2"></a>`distribution`: `riverhog-provenance`
- <a id="s-86923c6652"></a>`module`: `riverhog_provenance`
- <a id="s-9eaafe4739"></a>`name`: `observer_reference`
- <a id="s-44ba369c09"></a>`owner`: `riverhog_provenance.ResolvedProvenanceObserver`
- <a id="s-7af1983495"></a>`unit`: `member`

### Declared structure

- <a id="s-b82e9d9c12"></a>`kind`: `"method"`
- <a id="s-af26dc8abe"></a>`signature`: `"\"(self) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [ResolvedProvenanceObserver](riverhog-provenance-resolvedprovenanceobserver.md)

## Governing policies

- <a id="pa-2323d14107"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.ResolvedProvenanceObserver.observer_reference`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f431356e9697ddae0a208c03b7681c040873bced73d1b99b41d8353b4b3226a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "observer_reference",
  "owner": "riverhog_provenance.ResolvedProvenanceObserver",
  "unit": "member"
}
```

</details>
