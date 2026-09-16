# stove0_observer_protocol.ObserverDescriptor.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observerdescriptor-seal:b62592e0f3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d9f3061d16"></a>
- <a id="s-19999ec268"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-be49d2d021"></a>`module`: `stove0_observer_protocol`
- <a id="s-5e4b9ce465"></a>`name`: `seal`
- <a id="s-b42d46cddd"></a>`owner`: `stove0_observer_protocol.ObserverDescriptor`
- <a id="s-0fed2b6266"></a>`unit`: `member`

### Declared structure

- <a id="s-0e6f8d9e66"></a>`kind`: `"classmethod"`
- <a id="s-9eb21ce762"></a>`signature`: `"\"(cls, payload: 'ObserverDescriptorPayload') -> 'ObserverDescriptor'\""`

## Maintained corroboration

### Related interface records

- [ObserverDescriptor](stove0-observer-protocol-observerdescriptor.md)

## Governing policies

- <a id="pa-f98c38beff"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverDescriptor.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5ef75731420bee93a812e5508749d8442ae51168a1ce6288c2191ed195ff3043 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'ObserverDescriptorPayload') -> 'ObserverDescriptor'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "seal",
  "owner": "stove0_observer_protocol.ObserverDescriptor",
  "unit": "member"
}
```

</details>
