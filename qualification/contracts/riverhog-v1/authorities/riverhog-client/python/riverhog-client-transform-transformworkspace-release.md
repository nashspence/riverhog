# riverhog_client.transform.TransformWorkspace.release

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-transformworkspace-release:dbacc2865e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2bc06828b0"></a>
| Field | Shape |
|---|---|
| <a id="s-606aac04f2"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-7dc1c86857"></a>`distribution` | "riverhog-client" |
| <a id="s-d586b281fb"></a>`module` | "riverhog_client.transform" |
| <a id="s-c71d90230c"></a>`name` | "release" |
| <a id="s-0164b554b5"></a>`owner` | "riverhog_client.transform.TransformWorkspace" |
| <a id="s-f30cf91adf"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.TransformWorkspace](riverhog-client-transform-transformworkspace.md)

## Governing policies

- <a id="pa-5f84694ed6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.TransformWorkspace.release`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 65c10d8215c6584a27ecc24506ce93c0fd7b529f2a1fe518b7bae644005298fb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "release",
  "owner": "riverhog_client.transform.TransformWorkspace",
  "unit": "member"
}
```
