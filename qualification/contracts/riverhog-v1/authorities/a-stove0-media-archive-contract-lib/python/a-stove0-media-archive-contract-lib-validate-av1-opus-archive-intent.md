# a_stove0_media_archive_contract_lib.validate_av1_opus_archive_intent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-valid-650c76a94e:2c40af5f24 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5c5ab84943"></a>
- <a id="s-84640681e8"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-b3e9f0dd78"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-fb306bb53a"></a>`name`: `validate_av1_opus_archive_intent`
- <a id="s-621626022a"></a>`unit`: `export`

### Declared structure

- <a id="s-72346b50ca"></a>`kind`: `"function"`
- <a id="s-2787d95e3d"></a>`signature`: `"\"(intent: 'Mapping[str, object]') -> 'None'\""`

## Governing policies

- <a id="pa-182799c3ee"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.validate_av1_opus_archive_intent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b259a6909f7b22e1e052875a8527eda038ca8418838813f49fb2f12a15506bab -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(intent: 'Mapping[str, object]') -> 'None'\""
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "validate_av1_opus_archive_intent",
  "unit": "export"
}
```

</details>
