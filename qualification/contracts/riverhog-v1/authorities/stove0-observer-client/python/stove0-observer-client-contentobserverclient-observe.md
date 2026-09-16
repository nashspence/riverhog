# stove0_observer_client.ContentObserverClient.observe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-client:stove0-observer-client-contentobserverclient-observe:72cb900727 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4e7eeb7edf"></a>
- <a id="s-19f50f2b9b"></a>`distribution`: `stove0-observer-client`
- <a id="s-38e415a6dd"></a>`module`: `stove0_observer_client`
- <a id="s-305948cb2d"></a>`name`: `observe`
- <a id="s-9be868297c"></a>`owner`: `stove0_observer_client.ContentObserverClient`
- <a id="s-fc72369bc3"></a>`unit`: `member`

### Declared structure

- <a id="s-44baf8a06b"></a>`kind`: `"method"`
- <a id="s-341683faf5"></a>`signature`: `"\"(self, invocation: 'ObservationInvocation', *, descriptor: 'ObserverDescriptor') -> 'ObservationResult'\""`

## Maintained corroboration

### Related interface records

- [ContentObserverClient](stove0-observer-client-contentobserverclient.md)

## Governing policies

- <a id="pa-2109681afc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-client:stove0_observer_client](../../../evidence/sources.md#src-67dbe161ba) — `reference/stove0/packages/observer-client/src/stove0_observer_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_client.ContentObserverClient.observe`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 57ef48955e09c8847a28a435e243011072be9757e4f33d1df0d3ca12acfe434b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, invocation: 'ObservationInvocation', *, descriptor: 'ObserverDescriptor') -> 'ObservationResult'\""
  },
  "distribution": "stove0-observer-client",
  "module": "stove0_observer_client",
  "name": "observe",
  "owner": "stove0_observer_client.ContentObserverClient",
  "unit": "member"
}
```

</details>
