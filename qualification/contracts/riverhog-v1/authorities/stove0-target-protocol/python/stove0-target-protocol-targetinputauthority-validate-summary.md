# stove0_target_protocol.TargetInputAuthority.validate_summary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetinputauthori-e33f92d6d4:c5a24547e0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-803bc7a049"></a>
- <a id="s-256893a39b"></a>`distribution`: `stove0-target-protocol`
- <a id="s-8e350babac"></a>`module`: `stove0_target_protocol`
- <a id="s-42e51640e3"></a>`name`: `validate_summary`
- <a id="s-97717b19f7"></a>`owner`: `stove0_target_protocol.TargetInputAuthority`
- <a id="s-c50c25d83e"></a>`unit`: `member`

### Declared structure

- <a id="s-f6ff51bcf5"></a>`kind`: `"method"`
- <a id="s-2be6653adc"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetInputAuthority](stove0-target-protocol-targetinputauthority.md)

## Governing policies

- <a id="pa-e7c77eb17f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetInputAuthority.validate_summary`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a99f9d2c646666701070de3067c6a1b165f22c5859d33b522dd11f463987c2d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "validate_summary",
  "owner": "stove0_target_protocol.TargetInputAuthority",
  "unit": "member"
}
```
