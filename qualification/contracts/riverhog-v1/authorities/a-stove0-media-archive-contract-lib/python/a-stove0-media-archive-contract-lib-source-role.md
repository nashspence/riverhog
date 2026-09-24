# a_stove0_media_archive_contract_lib.SOURCE_ROLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-source-role:ef2f54e84e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c09460793f"></a>
- <a id="s-b970c4407e"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-b45178c473"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-6147691c3f"></a>`name`: `SOURCE_ROLE`
- <a id="s-e4e9a45646"></a>`unit`: `export`

### Declared structure

- <a id="s-6ed34b74bb"></a>`kind`: `"constant"`
- <a id="s-51142830ac"></a>`value`: `"stove0.media.source/v1"`

## Governing policies

- <a id="pa-fb02f0f519"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.SOURCE_ROLE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ec1808d578fede34c9bb8d9a6dc5b8610df3ff48013baa9ddb198f8c9688ed03 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.media.source/v1"
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "SOURCE_ROLE",
  "unit": "export"
}
```

</details>
