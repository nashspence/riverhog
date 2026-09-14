# stove0_core.Stove0RiverhogClient.acquire_preview_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-acquire-31332703d6:9b91828d94 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6f3098be6c"></a>
- <a id="s-8f73709119"></a>`distribution`: `stove0-server`
- <a id="s-68dbfbe1aa"></a>`module`: `stove0_core`
- <a id="s-b0c0eff0d5"></a>`name`: `acquire_preview_claim`
- <a id="s-0198ce8263"></a>`owner`: `stove0_core.Stove0RiverhogClient`
- <a id="s-a0d99e34fa"></a>`unit`: `member`

### Declared structure

- <a id="s-9b6a555ee9"></a>`kind`: `"method"`
- <a id="s-e49d880e64"></a>`signature`: `"\"(self, request: 'WorkflowPreviewRequest') -> 'ClaimBinding'\""`

## Maintained corroboration

### Related interface records

- [Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-c2361f65ff"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.acquire_preview_claim`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b89d328cd10fb8d6bd7a711205bf1ebc58f161bc56286a318cbefb6377d85e47 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WorkflowPreviewRequest') -> 'ClaimBinding'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "acquire_preview_claim",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```
