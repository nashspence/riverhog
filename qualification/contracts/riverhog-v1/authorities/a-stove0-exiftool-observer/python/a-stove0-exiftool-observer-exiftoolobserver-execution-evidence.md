# a_stove0_exiftool_observer.ExiftoolObserver.execution_evidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-exiftool-observer:a-stove0-exiftool-observer-exiftoolobserv-657b8d692c:bd9d0bc704 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-exiftool-observer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8281e5bfb8"></a>
- <a id="s-34e4e93b78"></a>`distribution`: `a-stove0-exiftool-observer`
- <a id="s-5bd420ff86"></a>`module`: `a_stove0_exiftool_observer`
- <a id="s-a39e856033"></a>`name`: `execution_evidence`
- <a id="s-99140276ef"></a>`owner`: `a_stove0_exiftool_observer.ExiftoolObserver`
- <a id="s-96ac358c2b"></a>`unit`: `member`

### Declared structure

- <a id="s-3f02f7c4c2"></a>`kind`: `"method"`
- <a id="s-927a3b3430"></a>`signature`: `"\"(self) -> 'dict[str, str]'\""`

## Maintained corroboration

### Related interface records

- [ExiftoolObserver](a-stove0-exiftool-observer-exiftoolobserver.md)

## Governing policies

- <a id="pa-7d995f7625"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-exiftool-observer:a_stove0_exiftool_observer](../../../evidence/sources/authorities.md#src-0aacfe9358) — [some-implementations/stove0/observers/exiftool/src/a\_stove0\_exiftool\_observer/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/exiftool/src/a_stove0_exiftool_observer/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_exiftool_observer.ExiftoolObserver.execution_evidence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 25e03718e6a0d6c415e200be2f50891f5ea2abad6648e490bc66eb41aa543bcd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, str]'\""
  },
  "distribution": "a-stove0-exiftool-observer",
  "module": "a_stove0_exiftool_observer",
  "name": "execution_evidence",
  "owner": "a_stove0_exiftool_observer.ExiftoolObserver",
  "unit": "member"
}
```

</details>
