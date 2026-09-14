# stove0_core.PreviewRiverhogPort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-previewriverhogport:8e059f89f5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c5dc3adefa"></a>
- <a id="s-aeb9b75aac"></a>`distribution`: `stove0-server`
- <a id="s-4c554eb10e"></a>`module`: `stove0_core`
- <a id="s-01d76291cb"></a>`name`: `PreviewRiverhogPort`
- <a id="s-cbaf8debfa"></a>`unit`: `export`

### Declared structure

- <a id="s-d60161f59a"></a>`kind`: `"class"`
- <a id="s-db80d3128f"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [abandon_preview_claim](stove0-core-previewriverhogport-abandon-preview-claim.md)
- [acquire_preview_claim](stove0-core-previewriverhogport-acquire-preview-claim.md)
- [observation_authority](stove0-core-previewriverhogport-observation-authority.md)

## Governing policies

- <a id="pa-600054d381"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.PreviewRiverhogPort`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7b45167c145537e0548af10c3725abf3f9e8a893b3d66a1e606d3e2e62a239eb -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "PreviewRiverhogPort",
  "unit": "export"
}
```
