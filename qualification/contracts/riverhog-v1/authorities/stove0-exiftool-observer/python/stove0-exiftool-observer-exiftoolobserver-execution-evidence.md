# stove0_exiftool_observer.ExiftoolObserver.execution_evidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-exiftool-observer:stove0-exiftool-observer-exiftoolobserver-4b85aa6387:be20b850a4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c39c7f246c"></a>
- <a id="s-c6e8800025"></a>`distribution`: `stove0-exiftool-observer`
- <a id="s-02da3d1e3e"></a>`module`: `stove0_exiftool_observer`
- <a id="s-4efd0c04b8"></a>`name`: `execution_evidence`
- <a id="s-e549de2518"></a>`owner`: `stove0_exiftool_observer.ExiftoolObserver`
- <a id="s-78f39424b9"></a>`unit`: `member`

### Declared structure

- <a id="s-1a41b4c4c0"></a>`kind`: `"method"`
- <a id="s-ed09f54c7a"></a>`signature`: `"\"(self) -> 'dict[str, str]'\""`

## Maintained corroboration

### Related interface records

- [ExiftoolObserver](stove0-exiftool-observer-exiftoolobserver.md)

## Governing policies

- <a id="pa-a43728af4e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-exiftool-observer:stove0_exiftool_observer](../../../evidence/sources/authorities.md#src-18b5d27762) — [reference/stove0/observers/exiftool/src/stove0\_exiftool\_observer/\_\_init\_\_.py](../../../../../../reference/stove0/observers/exiftool/src/stove0_exiftool_observer/__init__.py)

### Machine authority

- `/external_contract/python/stove0_exiftool_observer.ExiftoolObserver.execution_evidence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 44b742324154eadd6759d2cbdcaebc0d3ed086c4250b46874feb63963d733ff1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, str]'\""
  },
  "distribution": "stove0-exiftool-observer",
  "module": "stove0_exiftool_observer",
  "name": "execution_evidence",
  "owner": "stove0_exiftool_observer.ExiftoolObserver",
  "unit": "member"
}
```

</details>
