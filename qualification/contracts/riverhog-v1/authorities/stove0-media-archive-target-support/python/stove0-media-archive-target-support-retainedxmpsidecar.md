# stove0_media_archive_target_support.RetainedXmpSidecar

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-retai-45281bfa6b:6efe2a78fd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d6513df363"></a>
- <a id="s-28a051c2fa"></a>`distribution`: `stove0-media-archive-target-support`
- <a id="s-913f255888"></a>`module`: `stove0_media_archive_target_support`
- <a id="s-144fa21554"></a>`name`: `RetainedXmpSidecar`
- <a id="s-6d84c5f1c0"></a>`unit`: `export`

### Declared structure

- <a id="s-038c526b64"></a>`kind`: `"class"`
- <a id="s-d6dc11ada1"></a>`signature`: `"'(*, input_artifact_id: str, output_path: str) -> None'"`

#### Validated model schema

<a id="s-94200ecc6c"></a>
- <a id="s-aace336fbf"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-789057e36f"></a>`input_artifact_id` | yes | type="string" |  |
| <a id="s-2097892712"></a>`output_path` | yes | type="string" |  |

## Maintained corroboration

### Related interface records

- [stove0_media_archive_target_support.RetainedXmpSidecar.canonical_path](stove0-media-archive-target-support-retainedxmpsidecar-canonical-path.md)

## Governing policies

- <a id="pa-066b6a65a8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — `reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.RetainedXmpSidecar`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7e8aafcad501679856339b49064058a683aaf79a2aeb950dc5f83e5fb1346304 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "input_artifact_id": {
          "type": "string"
        },
        "output_path": {
          "type": "string"
        }
      },
      "required": [
        "input_artifact_id",
        "output_path"
      ],
      "type": "object"
    },
    "signature": "'(*, input_artifact_id: str, output_path: str) -> None'"
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "RetainedXmpSidecar",
  "unit": "export"
}
```
