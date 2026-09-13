# stove0_target_support.ExternalEffectReceiptPayload.bounded_result

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-externaleffectrecei-5ae5604c68:d0abe6634b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d7c949d255"></a>
| Field | Shape |
|---|---|
| <a id="s-99de2c6c1f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ff999c03e2"></a>`distribution` | "stove0-target-support" |
| <a id="s-ff7d4cc926"></a>`module` | "stove0_target_support" |
| <a id="s-46b1e1c554"></a>`name` | "bounded_result" |
| <a id="s-d59103b98b"></a>`owner` | "stove0_target_support.ExternalEffectReceiptPayload" |
| <a id="s-39db238ce6"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.ExternalEffectReceiptPayload](stove0-target-support-externaleffectreceiptpayload.md)

## Governing policies

- <a id="pa-c2b0eefc24"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.ExternalEffectReceiptPayload.bounded_result`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7ebc9baa9618e1dd64a641586100af323f93caf33d625b63a184eec0cd97c62f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "bounded_result",
  "owner": "stove0_target_support.ExternalEffectReceiptPayload",
  "unit": "member"
}
```
