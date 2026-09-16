# stove0_exiftool_observer.ExiftoolObserver.observe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-exiftool-observer:stove0-exiftool-observer-exiftoolobserver-observe:6a9a79630a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0ec06d7828"></a>
- <a id="s-4276252002"></a>`distribution`: `stove0-exiftool-observer`
- <a id="s-ff445efef6"></a>`module`: `stove0_exiftool_observer`
- <a id="s-8331f81359"></a>`name`: `observe`
- <a id="s-3ddc4c6623"></a>`owner`: `stove0_exiftool_observer.ExiftoolObserver`
- <a id="s-0af8062669"></a>`unit`: `member`

### Declared structure

- <a id="s-b82a9f8d33"></a>`kind`: `"method"`
- <a id="s-d13b076851"></a>`signature`: `"\"(self, request: 'ObservationRequest', runtime: 'ObservationRuntime') -> 'ObservationResult'\""`

## Maintained corroboration

### Related interface records

- [ExiftoolObserver](stove0-exiftool-observer-exiftoolobserver.md)

## Governing policies

- <a id="pa-d3ad2109e2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-exiftool-observer:stove0_exiftool_observer](../../../evidence/sources.md#src-18b5d27762) — `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/__init__.py`

### Machine authority

- `/external_contract/python/stove0_exiftool_observer.ExiftoolObserver.observe`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 34752699034a7ba13bd2e1926b33c5a5f514a12c859cdbd3c5df399ae4dcbec5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ObservationRequest', runtime: 'ObservationRuntime') -> 'ObservationResult'\""
  },
  "distribution": "stove0-exiftool-observer",
  "module": "stove0_exiftool_observer",
  "name": "observe",
  "owner": "stove0_exiftool_observer.ExiftoolObserver",
  "unit": "member"
}
```

</details>
