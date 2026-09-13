# riverhog_storage_adapter_protocol.WriteCompletionAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writeco-220f6362b5:f90fbdd150 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-05ee202efc"></a>
| Field | Shape |
|---|---|
| <a id="s-f65ab559f7"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-6601d0b76d"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-9f45d619fc"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-55ab86e4a3"></a>`name` | "WriteCompletionAuthority" |
| <a id="s-7c78dfeffd"></a>`unit` | "export" |

## Governing policies

- <a id="pa-38dff110ff"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteCompletionAuthority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e3cd30c18486d019cc7baaa83a2b29946165b6fe56892ffb970cf1b295e0334e -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "c522a1b0d2d807b0740cab37856acf28b1c1eea4fa6a576110441995d96589d7",
    "signature": "'(*, segment_count: Annotated[int, Ge(ge=0)], stored_bytes: Annotated[int, Ge(ge=0)], authority_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)]) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteCompletionAuthority",
  "unit": "export"
}
```
