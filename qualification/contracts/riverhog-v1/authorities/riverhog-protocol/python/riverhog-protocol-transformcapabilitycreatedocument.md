# riverhog_protocol.TransformCapabilityCreateDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-transformcapabilitycreatedocument:0927b03139 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d6e7d0fcc2"></a>
| Field | Shape |
|---|---|
| <a id="s-d71b5b3cc4"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-382ca82e8a"></a>`distribution` | "riverhog-protocol" |
| <a id="s-8f590c17c3"></a>`module` | "riverhog_protocol" |
| <a id="s-c22c117ca3"></a>`name` | "TransformCapabilityCreateDocument" |
| <a id="s-866d47f293"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.TransformCapabilityCreateDocument.validate_capability](riverhog-protocol-transformcapabilitycreatedocument-validate-capability.md)

## Governing policies

- <a id="pa-eedecf10b4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.TransformCapabilityCreateDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b662ed67da5623d19ccd74ee389b5ad0c1b5ec0204f7d95d4629845a9ce13ba3 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "017e09367938982b92e94f62bb0ccb5a6249b1f8a8ecf328be4883bbd0b76738",
    "signature": "\"(*, fence: Annotated[int, Ge(ge=1)], audience: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9][a-z0-9._:/-]{0,299}$')], actions: Annotated[list[Literal['read-inputs', 'write-output']], MinLen(min_length=1)] = <factory>, ttl_seconds: Annotated[int, Ge(ge=30), Le(le=86400)] = 900) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "TransformCapabilityCreateDocument",
  "unit": "export"
}
```
