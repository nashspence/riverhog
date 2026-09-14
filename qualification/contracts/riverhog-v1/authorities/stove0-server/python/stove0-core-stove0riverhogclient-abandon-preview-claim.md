# stove0_core.Stove0RiverhogClient.abandon_preview_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-abandon-f7b59db3d5:53a1a37363 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-95fc7403ab"></a>
- <a id="s-a7a3ca05d7"></a>`distribution`: `stove0-server`
- <a id="s-c5109aaaf9"></a>`module`: `stove0_core`
- <a id="s-2a1e3dc2cf"></a>`name`: `abandon_preview_claim`
- <a id="s-5c50cc37eb"></a>`owner`: `stove0_core.Stove0RiverhogClient`
- <a id="s-6205d4f08c"></a>`unit`: `member`

### Declared structure

- <a id="s-7052af64fb"></a>`kind`: `"method"`
- <a id="s-7eaac03049"></a>`signature`: `"\"(self, request: 'WorkflowPreviewRequest', claim: 'ClaimBinding') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-5ee40993ba"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.abandon_preview_claim`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d394ff1f2adce3a9cc8dc877f18c7bb8ad8347a6dc79a1f3ec813baa23e7096e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WorkflowPreviewRequest', claim: 'ClaimBinding') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "abandon_preview_claim",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```
