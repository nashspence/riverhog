# stove0_target_protocol.TargetCallbackAccess

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetcallbackaccess:292785ebcf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2aa91b22b2"></a>
| Field | Shape |
|---|---|
| <a id="s-56956595ca"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-47a1aa2aa7"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-64db854d1a"></a>`module` | "stove0_target_protocol" |
| <a id="s-f0b9de85dc"></a>`name` | "TargetCallbackAccess" |
| <a id="s-84420f33f1"></a>`unit` | "export" |

## Governing policies

- <a id="pa-efb7bf6beb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetCallbackAccess`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 40647e79d45b87f8e815681321abd6d2b98efb8ecb695061fb69f97737769b87 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "b19c85f349461f7bea0f4a19f4912f9f84ad2465cca72d713ad8feebea0854ac",
    "signature": "'(*, stove0_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False) -> None'"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetCallbackAccess",
  "unit": "export"
}
```
