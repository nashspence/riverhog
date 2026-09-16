# stove0_target_protocol.ExternalEffectReceipt.bounded_result

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-externaleffectrece-274e1532ff:9eb5302aef -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e1d39707e3"></a>
- <a id="s-0a102894ff"></a>`distribution`: `stove0-target-protocol`
- <a id="s-1bcb54f557"></a>`module`: `stove0_target_protocol`
- <a id="s-c887354a12"></a>`name`: `bounded_result`
- <a id="s-9b8fabdeec"></a>`owner`: `stove0_target_protocol.ExternalEffectReceipt`
- <a id="s-5b0e52d70d"></a>`unit`: `member`

### Declared structure

- <a id="s-708f7d4e74"></a>`kind`: `"method"`
- <a id="s-8dc63e7cf8"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ExternalEffectReceipt](stove0-target-protocol-externaleffectreceipt.md)

## Governing policies

- <a id="pa-e8c78be74d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.ExternalEffectReceipt.bounded_result`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 97c84e6ad28ebba92230617316601620c039c71c6c654e2daf7af6445b657025 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "bounded_result",
  "owner": "stove0_target_protocol.ExternalEffectReceipt",
  "unit": "member"
}
```
