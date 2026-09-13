# riverhog_client.transform.CapabilityApiClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-capabilityapiclient:9cbd8f7c18 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-94c713ae5e"></a>
| Field | Shape |
|---|---|
| <a id="s-81596b885c"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-2541027372"></a>`distribution` | "riverhog-client" |
| <a id="s-89c3bf6284"></a>`module` | "riverhog_client.transform" |
| <a id="s-143c4ce839"></a>`name` | "CapabilityApiClient" |
| <a id="s-e890acd831"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.CapabilityApiClient.current](riverhog-client-transform-capabilityapiclient-current.md)
- [riverhog_client.transform.CapabilityApiClient.__getattr__](riverhog-client-transform-capabilityapiclient-getattr.md)
- [riverhog_client.transform.CapabilityApiClient.replace](riverhog-client-transform-capabilityapiclient-replace.md)
- [riverhog_client.transform.CapabilityApiClient.close](riverhog-client-transform-capabilityapiclient-close.md)
- [riverhog_client.transform.CapabilityApiClient.__enter__](riverhog-client-transform-capabilityapiclient-enter.md)
- [riverhog_client.transform.CapabilityApiClient.__exit__](riverhog-client-transform-capabilityapiclient-exit.md)
- [riverhog_client.transform.CapabilityApiClient.spawn](riverhog-client-transform-capabilityapiclient-spawn.md)

## Governing policies

- <a id="pa-690b1d6a61"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.CapabilityApiClient`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 090021e01064fb22dcd3b3307a98fbcf4bc7d299543e44856166515a62f587aa -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(client: 'Any', *, owns_client: 'bool' = False, _state: '_CapabilityClientState | None' = None, _root: 'bool' = True) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "CapabilityApiClient",
  "unit": "export"
}
```
