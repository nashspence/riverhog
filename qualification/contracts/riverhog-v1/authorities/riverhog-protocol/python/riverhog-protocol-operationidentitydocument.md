# riverhog_protocol.OperationIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-operationidentitydocument:18f5859546 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fcbbbc44c7"></a>
| Field | Shape |
|---|---|
| <a id="s-05d8aa21b4"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-f31de61ef2"></a>`distribution` | "riverhog-protocol" |
| <a id="s-47669e91dd"></a>`module` | "riverhog_protocol" |
| <a id="s-6f99ad08f3"></a>`name` | "OperationIdentityDocument" |
| <a id="s-e2bfaac9d0"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.OperationIdentityDocument.validate_identity](riverhog-protocol-operationidentitydocument-validate-identity.md)

## Governing policies

- <a id="pa-7cfb8e10c1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.OperationIdentityDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c962b9676818a1ab254fff7a98cf0e4d4d19faca4358f432d690580fa85bb764 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "8edceb5ac10853866a052a46257f6386a34c5bd639e7416a8eabfdb516ec77a0",
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "OperationIdentityDocument",
  "unit": "export"
}
```
