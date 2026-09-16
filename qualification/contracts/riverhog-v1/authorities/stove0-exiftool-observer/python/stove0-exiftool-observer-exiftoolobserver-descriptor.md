# stove0_exiftool_observer.ExiftoolObserver.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-exiftool-observer:stove0-exiftool-observer-exiftoolobserver-descriptor:49e0aecd94 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-06eabfa36b"></a>
- <a id="s-8383d7689a"></a>`distribution`: `stove0-exiftool-observer`
- <a id="s-c59ae7ba2e"></a>`module`: `stove0_exiftool_observer`
- <a id="s-91564b0bac"></a>`name`: `descriptor`
- <a id="s-5751066f82"></a>`owner`: `stove0_exiftool_observer.ExiftoolObserver`
- <a id="s-9b43d85e19"></a>`unit`: `member`

### Declared structure

- <a id="s-b096056f2a"></a>`kind`: `"method"`
- <a id="s-253122c44e"></a>`signature`: `"\"(self) -> 'ObserverDescriptor'\""`

## Maintained corroboration

### Related interface records

- [ExiftoolObserver](stove0-exiftool-observer-exiftoolobserver.md)

## Governing policies

- <a id="pa-b92dc6a41e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-exiftool-observer:stove0_exiftool_observer](../../../evidence/sources.md#src-18b5d27762) — `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/__init__.py`

### Machine authority

- `/external_contract/python/stove0_exiftool_observer.ExiftoolObserver.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 26ee41d947c153f0524bb824eb360fef6ee9204ec74a1d9d0b81f4f61d713c91 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'ObserverDescriptor'\""
  },
  "distribution": "stove0-exiftool-observer",
  "module": "stove0_exiftool_observer",
  "name": "descriptor",
  "owner": "stove0_exiftool_observer.ExiftoolObserver",
  "unit": "member"
}
```

</details>
