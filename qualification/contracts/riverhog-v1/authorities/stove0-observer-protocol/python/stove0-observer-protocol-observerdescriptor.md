# stove0_observer_protocol.ObserverDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observerdescriptor:0d772f46fc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ea4ccf8767"></a>
| Field | Shape |
|---|---|
| <a id="s-6b1221af31"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-60e459675a"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-cc57cdf4e9"></a>`module` | "stove0_observer_protocol" |
| <a id="s-f4f7a54b0d"></a>`name` | "ObserverDescriptor" |
| <a id="s-e37aa6e119"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.ObserverDescriptor.verify_digest](stove0-observer-protocol-observerdescriptor-verify-digest.md)
- [stove0_observer_protocol.ObserverDescriptor.support_for](stove0-observer-protocol-observerdescriptor-support-for.md)
- [stove0_observer_protocol.ObserverDescriptor.seal](stove0-observer-protocol-observerdescriptor-seal.md)

## Governing policies

- <a id="pa-3956c6da1e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverDescriptor`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fbebf3efc749c6ba436a3d633dc39dfe3b1f5dc212724910b2e1354ce7614eed -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "5717766018649fca58b8d29b78adfc61b4a572a9a7d959ac997b6f9756460f41",
    "signature": "\"(*, protocol: Literal['stove0-content-observer/v1'] = 'stove0-content-observer/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], contracts: Annotated[tuple[stove0_protocol.models.ObserverContractSupport, ...], MinLen(min_length=1)], descriptor_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObserverDescriptor",
  "unit": "export"
}
```
