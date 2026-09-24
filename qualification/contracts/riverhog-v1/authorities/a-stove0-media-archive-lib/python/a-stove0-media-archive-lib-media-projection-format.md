# a_stove0_media_archive_lib.MEDIA_PROJECTION_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-media-projection-format:a67dc6635b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fbed8d175b"></a>
- <a id="s-1834cb4a9a"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-50a9c33f23"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-2f386d5f67"></a>`name`: `MEDIA_PROJECTION_FORMAT`
- <a id="s-91015ce726"></a>`unit`: `export`

### Declared structure

- <a id="s-5e8ea0751c"></a>`kind`: `"constant"`
- <a id="s-db6b8570f7"></a>`value`: `"stove0-media-archive-projection/v1"`

## Governing policies

- <a id="pa-d3e3c13a53"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.MEDIA_PROJECTION_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: deb3a54070ed4595323e039e489a3676dd87208d11d5ecfc38c526417d335196 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-media-archive-projection/v1"
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "MEDIA_PROJECTION_FORMAT",
  "unit": "export"
}
```

</details>
