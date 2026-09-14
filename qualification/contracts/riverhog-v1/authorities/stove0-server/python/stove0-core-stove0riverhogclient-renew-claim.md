# stove0_core.Stove0RiverhogClient.renew_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-renew-claim:57b89e60b6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-46d153dcca"></a>
- <a id="s-e256ea6e33"></a>`distribution`: `stove0-server`
- <a id="s-227cea8e75"></a>`module`: `stove0_core`
- <a id="s-b97c68a17d"></a>`name`: `renew_claim`
- <a id="s-0214506a49"></a>`owner`: `stove0_core.Stove0RiverhogClient`
- <a id="s-324d1f7845"></a>`unit`: `member`

### Declared structure

- <a id="s-d9f76e0249"></a>`kind`: `"method"`
- <a id="s-10e6989332"></a>`signature`: `"\"(self, work: 'WorkIdentity', claim: 'ClaimBinding') -> 'ClaimBinding'\""`

## Maintained corroboration

### Related interface records

- [Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-d99cc94b08"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.renew_claim`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8eb29b9fcd3de33b86bd14d68fe630964d7aa09f479c1a36fde77d55c06a9503 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work: 'WorkIdentity', claim: 'ClaimBinding') -> 'ClaimBinding'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "renew_claim",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```
