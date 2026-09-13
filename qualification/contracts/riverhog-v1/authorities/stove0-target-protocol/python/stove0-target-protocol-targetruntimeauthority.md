# stove0_target_protocol.TargetRuntimeAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetruntimeauthority:2d89e78dd2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c78ba96a4b"></a>
| Field | Shape |
|---|---|
| <a id="s-7511a85446"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-55c88a69d7"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-0a75671fe6"></a>`module` | "stove0_target_protocol" |
| <a id="s-ec73fecf7a"></a>`name` | "TargetRuntimeAuthority" |
| <a id="s-69030aacfc"></a>`unit` | "export" |

## Governing policies

- <a id="pa-df597579f2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetRuntimeAuthority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6494aa1c1243fc047da6377e2206bb1f0296ba1c72ffc28469c8b1e9766c0dbf -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "6ac4b25b7019574c8a208af672bd8cd7caa8a1b22ac48d6ddf6946379a4bafa7",
    "signature": "\"(*, transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', riverhog_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], capability_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetRuntimeAuthority",
  "unit": "export"
}
```
