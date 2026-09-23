# a_stove0_exiftool_observer.ExiftoolObserver

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-exiftool-observer:a-stove0-exiftool-observer-exiftoolobserver:e328bdab06 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-exiftool-observer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fe287c800c"></a>
- <a id="s-7994d7c11e"></a>`distribution`: `a-stove0-exiftool-observer`
- <a id="s-a453ee1f09"></a>`module`: `a_stove0_exiftool_observer`
- <a id="s-460bb0c9cc"></a>`name`: `ExiftoolObserver`
- <a id="s-2360122676"></a>`unit`: `export`

### Declared structure

- <a id="s-5e2f595f51"></a>`kind`: `"class"`
- <a id="s-9d0d05e3b2"></a>`signature`: `"\"(*, exiftool: 'str' = 'exiftool', workspace_root: 'Path \| None' = None, source_revision: 'str' = 'unknown', image_id: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [descriptor](a-stove0-exiftool-observer-exiftoolobserver-descriptor.md)
- [execution_evidence](a-stove0-exiftool-observer-exiftoolobserver-execution-evidence.md)
- [observe](a-stove0-exiftool-observer-exiftoolobserver-observe.md)

## Governing policies

- <a id="pa-e2e3086ec4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-exiftool-observer:a_stove0_exiftool_observer](../../../evidence/sources/authorities.md#src-0aacfe9358) — [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_exiftool_observer.ExiftoolObserver`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 326f039434f5f3b679e30bc1538b5964f980b481d983d6c5469df0515efee0d1 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, exiftool: 'str' = 'exiftool', workspace_root: 'Path | None' = None, source_revision: 'str' = 'unknown', image_id: 'str') -> 'None'\""
  },
  "distribution": "a-stove0-exiftool-observer",
  "module": "a_stove0_exiftool_observer",
  "name": "ExiftoolObserver",
  "unit": "export"
}
```

</details>
