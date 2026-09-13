# riverhog_protocol.CollectionUploadRegistrationConstraintsDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadregistr-54809f5fa2:a422dac2f2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-08808e03e1"></a>
| Field | Shape |
|---|---|
| <a id="s-37b608a985"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-3fab0c3210"></a>`distribution` | "riverhog-protocol" |
| <a id="s-39f68a6bca"></a>`module` | "riverhog_protocol" |
| <a id="s-882861bb83"></a>`name` | "CollectionUploadRegistrationConstraintsDocument" |
| <a id="s-b8df20de32"></a>`unit` | "export" |

## Governing policies

- <a id="pa-db5ce69555"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadRegistrationConstraintsDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fc6a06caa6ab7102c75be82cc14f76eba5b6af79af3e29493bfed703999fe130 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "ae13134c7a695730242fea7eaa2696741bba53ade562151ca727365c978fcfed",
    "signature": "'(*, pack_member_bytes: Annotated[int, Ge(ge=1)], raw_part_plaintext_bytes: Annotated[int, Ge(ge=65536), MultipleOf(multiple_of=65536)]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadRegistrationConstraintsDocument",
  "unit": "export"
}
```
