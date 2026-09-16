# stove0_observer_protocol.ObservationInvocation.canonical_claim_id

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationinvoc-58a94a7851:2180dd6a33 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7e7656a465"></a>
- <a id="s-23e5e5728d"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-2f0cc967be"></a>`module`: `stove0_observer_protocol`
- <a id="s-5feffa81fc"></a>`name`: `canonical_claim_id`
- <a id="s-ebffb2175a"></a>`owner`: `stove0_observer_protocol.ObservationInvocation`
- <a id="s-a6e6db7fb4"></a>`unit`: `member`

### Declared structure

- <a id="s-4c42500778"></a>`kind`: `"classmethod"`
- <a id="s-7c32821137"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [ObservationInvocation](stove0-observer-protocol-observationinvocation.md)

## Governing policies

- <a id="pa-85fff2aeab"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationInvocation.canonical_claim_id`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cee523027ddf8d3b75bc3f9d48deed3937ab2159291e0afd4b04ecd817dc0d17 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "canonical_claim_id",
  "owner": "stove0_observer_protocol.ObservationInvocation",
  "unit": "member"
}
```

</details>
