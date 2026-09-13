# stove0_observer_client.ContentObserverClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-client:stove0-observer-client-contentobserverclient:aaddc227bc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9e67030a39"></a>
| Field | Shape |
|---|---|
| <a id="s-2547f951e4"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-c76661d254"></a>`distribution` | "stove0-observer-client" |
| <a id="s-9c98cf6f51"></a>`module` | "stove0_observer_client" |
| <a id="s-e88328c912"></a>`name` | "ContentObserverClient" |
| <a id="s-110e0c0087"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_observer_client.ContentObserverClient.descriptor](stove0-observer-client-contentobserverclient-descriptor.md)
- [stove0_observer_client.ContentObserverClient.observe](stove0-observer-client-contentobserverclient-observe.md)

## Governing policies

- <a id="pa-b4bab6ed69"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-client:stove0_observer_client](../../../evidence/sources.md#src-67dbe161ba) — `reference/stove0/packages/observer-client/src/stove0_observer_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_client.ContentObserverClient`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d6a8a68275a0b2d9da3669547398771a01be57134b30944b933079e180c11a0e -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(base_url: 'str', *, token: 'str | None' = None, timeout: 'float | None' = 300.0, allow_insecure_http: 'bool' = False, semantic_validators: 'SemanticValidatorProvider | None' = None) -> 'None'\""
  },
  "distribution": "stove0-observer-client",
  "module": "stove0_observer_client",
  "name": "ContentObserverClient",
  "unit": "export"
}
```
