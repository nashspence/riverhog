# a_stove0_exiftool_observer.ExiftoolObserver.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-exiftool-observer:a-stove0-exiftool-observer-exiftoolobserv-28fe6c713e:2a698db278 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-exiftool-observer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e9621b670e"></a>
- <a id="s-1e5144570a"></a>`distribution`: `a-stove0-exiftool-observer`
- <a id="s-75651ec1c8"></a>`module`: `a_stove0_exiftool_observer`
- <a id="s-be86ab295a"></a>`name`: `descriptor`
- <a id="s-397418a6c6"></a>`owner`: `a_stove0_exiftool_observer.ExiftoolObserver`
- <a id="s-bde3153a35"></a>`unit`: `member`

### Declared structure

- <a id="s-3402b1299b"></a>`kind`: `"method"`
- <a id="s-f496ddd86a"></a>`signature`: `"\"(self) -> 'ObserverDescriptor'\""`

## Maintained corroboration

### Related interface records

- [ExiftoolObserver](a-stove0-exiftool-observer-exiftoolobserver.md)

## Governing policies

- <a id="pa-4093ad4cad"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-exiftool-observer:a_stove0_exiftool_observer](../../../evidence/sources/authorities.md#src-0aacfe9358) — [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_exiftool_observer.ExiftoolObserver.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6063affd65a5d5c6191f46b628970591b6ac27e649fa6b039bf875d0a1d0e950 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'ObserverDescriptor'\""
  },
  "distribution": "a-stove0-exiftool-observer",
  "module": "a_stove0_exiftool_observer",
  "name": "descriptor",
  "owner": "a_stove0_exiftool_observer.ExiftoolObserver",
  "unit": "member"
}
```

</details>
