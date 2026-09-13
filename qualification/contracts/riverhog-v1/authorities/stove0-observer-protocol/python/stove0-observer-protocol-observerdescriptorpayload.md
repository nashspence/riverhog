# stove0_observer_protocol.ObserverDescriptorPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observerdescriptorpayload:d89286f625 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e50183a98e"></a>
| Field | Shape |
|---|---|
| <a id="s-0b30b32b5d"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-ebe5e502f5"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-6cd091d081"></a>`module` | "stove0_observer_protocol" |
| <a id="s-20cb2ac692"></a>`name` | "ObserverDescriptorPayload" |
| <a id="s-b272ca1f7c"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.ObserverDescriptorPayload.unique_contracts](stove0-observer-protocol-observerdescriptorpayload-unique-contracts.md)

## Governing policies

- <a id="pa-83085a237c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverDescriptorPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 534d0b1cc7f8016467482441e9a9f6c8cd79b75ca5f0c3095388ed9a4d0fb724 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "96b0440bb530423fbeb0b6687c18f7f2fff67acc33d26df0e36018d29674e8ae",
    "signature": "\"(*, protocol: Literal['stove0-content-observer/v1'] = 'stove0-content-observer/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], contracts: Annotated[tuple[stove0_protocol.models.ObserverContractSupport, ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObserverDescriptorPayload",
  "unit": "export"
}
```
