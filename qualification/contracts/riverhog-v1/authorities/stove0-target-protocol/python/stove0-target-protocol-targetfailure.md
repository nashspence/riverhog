# stove0_target_protocol.TargetFailure

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetfailure:7032489ca7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-acd8fc45cd"></a>
| Field | Shape |
|---|---|
| <a id="s-0f8c52d1e9"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-54d136f0bc"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-45ae4ed44e"></a>`module` | "stove0_target_protocol" |
| <a id="s-ee465b971a"></a>`name` | "TargetFailure" |
| <a id="s-5ab5ecaea5"></a>`unit` | "export" |

## Governing policies

- <a id="pa-a84b22f929"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetFailure`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 18b1d12dfed077858d9832aa2b8c7c5ae3313c7fcca8045429dd2fd8c46192af -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "1f3471f4274fa0dc57b46bac02964b1e6b6f07bbd8a9a19f618b2031ccb0c065",
    "signature": "\"(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetFailure",
  "unit": "export"
}
```
