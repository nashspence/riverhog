# riverhog_client.transform.ClaimedRetrieval.read_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedretrieva-f6262bc04c:aa59bb4437 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2d0434fe53"></a>
| Field | Shape |
|---|---|
| <a id="s-4a0e1e24e2"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-e7e58d9e2d"></a>`distribution` | "riverhog-client" |
| <a id="s-ae49714776"></a>`module` | "riverhog_client.transform" |
| <a id="s-58819a886d"></a>`name` | "read_bytes" |
| <a id="s-2ab8724aa6"></a>`owner` | "riverhog_client.transform.ClaimedRetrieval" |
| <a id="s-b69117db73"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedRetrieval](riverhog-client-transform-claimedretrieval.md)

## Governing policies

- <a id="pa-169adf03a2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedRetrieval.read_bytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3edd431f2a0b041f4f4f43ebacbba6199a743a80091501e8efb7b00e723bb116 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, artifact: 'ClaimedArtifact', *, maximum_bytes: 'int') -> 'bytes'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "read_bytes",
  "owner": "riverhog_client.transform.ClaimedRetrieval",
  "unit": "member"
}
```
