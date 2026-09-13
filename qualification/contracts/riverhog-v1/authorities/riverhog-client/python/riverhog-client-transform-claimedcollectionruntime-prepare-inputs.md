# riverhog_client.transform.ClaimedCollectionRuntime.prepare_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-5b45cf8717:62c4f4649d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-911b1a4764"></a>
| Field | Shape |
|---|---|
| <a id="s-5062437b9f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-418daf9a6a"></a>`distribution` | "riverhog-client" |
| <a id="s-ad96eed345"></a>`module` | "riverhog_client.transform" |
| <a id="s-f3e2995879"></a>`name` | "prepare_inputs" |
| <a id="s-b415b4681b"></a>`owner` | "riverhog_client.transform.ClaimedCollectionRuntime" |
| <a id="s-950d11c1ca"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedCollectionRuntime](riverhog-client-transform-claimedcollectionruntime.md)

## Governing policies

- <a id="pa-bd61c5b822"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionRuntime.prepare_inputs`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 96a93249389c6d9d7c44338adae955c98062b64c0bc795450d9cf57c61d58bc5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, artifacts: 'Sequence[ClaimedArtifact] | None' = None, **kwargs: 'Any') -> 'ClaimedRetrieval'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "prepare_inputs",
  "owner": "riverhog_client.transform.ClaimedCollectionRuntime",
  "unit": "member"
}
```
