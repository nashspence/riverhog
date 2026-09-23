# a_stove0_exiftool_observer.ExiftoolObserver.observe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-exiftool-observer:a-stove0-exiftool-observer-exiftoolobserver-observe:8f9c1fc5b4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-exiftool-observer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-993107688c"></a>
- <a id="s-b5a6656fbe"></a>`distribution`: `a-stove0-exiftool-observer`
- <a id="s-4c40a61c8b"></a>`module`: `a_stove0_exiftool_observer`
- <a id="s-aa84e5a043"></a>`name`: `observe`
- <a id="s-ae99452e24"></a>`owner`: `a_stove0_exiftool_observer.ExiftoolObserver`
- <a id="s-046f479209"></a>`unit`: `member`

### Declared structure

- <a id="s-a4061e5eef"></a>`kind`: `"method"`
- <a id="s-460bb07083"></a>`signature`: `"\"(self, request: 'ContentObservationRequest', runtime: 'ContentObservationRuntime') -> 'ContentObservationResult'\""`

## Maintained corroboration

### Related interface records

- [ExiftoolObserver](a-stove0-exiftool-observer-exiftoolobserver.md)

## Governing policies

- <a id="pa-1b8603bb72"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-exiftool-observer:a_stove0_exiftool_observer](../../../evidence/sources/authorities.md#src-0aacfe9358) — [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_exiftool_observer.ExiftoolObserver.observe`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a7db52c55e7704e56270564f4fb92d2911c122fbeac3aed92d0a49920f3c4560 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ContentObservationRequest', runtime: 'ContentObservationRuntime') -> 'ContentObservationResult'\""
  },
  "distribution": "a-stove0-exiftool-observer",
  "module": "a_stove0_exiftool_observer",
  "name": "observe",
  "owner": "a_stove0_exiftool_observer.ExiftoolObserver",
  "unit": "member"
}
```

</details>
