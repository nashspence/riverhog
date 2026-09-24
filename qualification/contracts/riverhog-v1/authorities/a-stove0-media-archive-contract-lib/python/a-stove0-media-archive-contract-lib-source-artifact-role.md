# a_stove0_media_archive_contract_lib.SOURCE_ARTIFACT_ROLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-sourc-721f9c4281:836aaf067b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e7414c0fcd"></a>
- <a id="s-1af6e06de2"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-1dfe961273"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-bcbd39ac92"></a>`name`: `SOURCE_ARTIFACT_ROLE`
- <a id="s-7fdf2b1f2b"></a>`unit`: `export`

### Declared structure

- <a id="s-4617626def"></a>`kind`: `"constant"`
- <a id="s-19e34a18d6"></a>`value`: `"stove0.media.source-artifact/v1"`

## Governing policies

- <a id="pa-61072e3378"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.SOURCE_ARTIFACT_ROLE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5e28cd582addf87a1683395daa0aa13cb19acb726f231b38f92571d4f84c7cd8 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.media.source-artifact/v1"
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "SOURCE_ARTIFACT_ROLE",
  "unit": "export"
}
```

</details>
