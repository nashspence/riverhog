# a_stove0_media_archive_lib.render_projection_xmp

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-render-projection-xmp:e00ab29b82 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-598a702323"></a>
- <a id="s-07dbbe236f"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-8f6dd12603"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-c0583363cf"></a>`name`: `render_projection_xmp`
- <a id="s-bd2159a78b"></a>`unit`: `export`

### Declared structure

- <a id="s-4f7468a3c4"></a>`kind`: `"function"`
- <a id="s-83c3032ddf"></a>`signature`: `"\"(item: 'MediaProjectionItem', *, tags: 'Sequence[str]' = ()) -> 'bytes'\""`

## Governing policies

- <a id="pa-60c64a2b2b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.render_projection_xmp`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cb05962d718cb9a125bd94e519fbcbfbf092d70496271f016a1e31db226121af -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(item: 'MediaProjectionItem', *, tags: 'Sequence[str]' = ()) -> 'bytes'\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "render_projection_xmp",
  "unit": "export"
}
```

</details>
