# stove0_core.PreviewRiverhogPort.acquire_preview_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-previewriverhogport-acquire-p-55b0e8d0fc:f78435f252 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6e761a1361"></a>
- <a id="s-dba7dab323"></a>`distribution`: `stove0-server`
- <a id="s-3b61970921"></a>`module`: `stove0_core`
- <a id="s-e491355aaa"></a>`name`: `acquire_preview_claim`
- <a id="s-ed3ea9b53e"></a>`owner`: `stove0_core.PreviewRiverhogPort`
- <a id="s-a603ec0d91"></a>`unit`: `member`

### Declared structure

- <a id="s-7b503f8ecd"></a>`kind`: `"method"`
- <a id="s-d1967ad08c"></a>`signature`: `"\"(self, request: 'WorkflowPreviewRequest') -> 'ClaimBinding'\""`

## Maintained corroboration

### Related interface records

- [PreviewRiverhogPort](stove0-core-previewriverhogport.md)

## Governing policies

- <a id="pa-40da69fe55"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.PreviewRiverhogPort.acquire_preview_claim`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 533d2add6c0cbe3e55a2c22007f8248775d2b7d4ad9ba09f180ebd087b547b9d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WorkflowPreviewRequest') -> 'ClaimBinding'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "acquire_preview_claim",
  "owner": "stove0_core.PreviewRiverhogPort",
  "unit": "member"
}
```
