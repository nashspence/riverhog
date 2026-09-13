# riverhog_client.transform.Heartbeat

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-heartbeat:d27799c0d9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-503b606926"></a>
| Field | Shape |
|---|---|
| <a id="s-68129599fe"></a>`contract` | type="collections.abc._CallableGenericAlias"; additional keys=`kind` |
| <a id="s-70884b5874"></a>`distribution` | "riverhog-client" |
| <a id="s-26a660b91d"></a>`module` | "riverhog_client.transform" |
| <a id="s-ef980fd1e3"></a>`name` | "Heartbeat" |
| <a id="s-3e58b4c081"></a>`unit` | "export" |

## Governing policies

- <a id="pa-00de561c99"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.Heartbeat`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c361c894b8bee9995eea015e18cfb8ddd9b8a7a486629c3e41646b136a10a91c -->

```json
{
  "contract": {
    "kind": "object",
    "type": "collections.abc._CallableGenericAlias"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "Heartbeat",
  "unit": "export"
}
```
