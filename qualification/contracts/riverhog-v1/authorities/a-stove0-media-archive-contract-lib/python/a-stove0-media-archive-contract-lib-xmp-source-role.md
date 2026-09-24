# a_stove0_media_archive_contract_lib.XMP_SOURCE_ROLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-xmp-source-role:2e780c2282 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3f0994c181"></a>
- <a id="s-20cd147daf"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-eb21b77a6f"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-55ba7dadb5"></a>`name`: `XMP_SOURCE_ROLE`
- <a id="s-9695a0fb38"></a>`unit`: `export`

### Declared structure

- <a id="s-016d5c2a65"></a>`kind`: `"constant"`
- <a id="s-8352a0610e"></a>`value`: `"stove0.media.xmp-source/v1"`

## Governing policies

- <a id="pa-a476a57e43"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.XMP_SOURCE_ROLE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 941b099fb6ca11a5e883e0c79afc1909287699ecf2412c1967c04027851dc912 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.media.xmp-source/v1"
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "XMP_SOURCE_ROLE",
  "unit": "export"
}
```

</details>
