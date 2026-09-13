# stove0_target_support.TargetRuntimeAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetruntimeauthority:3ade464029 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-caafd157a0"></a>
| Field | Shape |
|---|---|
| <a id="s-8e04ae3ef0"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-f5661eecd8"></a>`distribution` | "stove0-target-support" |
| <a id="s-260ec80b64"></a>`module` | "stove0_target_support" |
| <a id="s-2c517c6f3b"></a>`name` | "TargetRuntimeAuthority" |
| <a id="s-2a53a0bb3e"></a>`unit` | "export" |

## Governing policies

- <a id="pa-1ba35d71e2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetRuntimeAuthority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ff95c7441d56eb236d73e1c3d9cb7b3b9ef27bcc859cc0f76eb4309b60bdf800 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "6ac4b25b7019574c8a208af672bd8cd7caa8a1b22ac48d6ddf6946379a4bafa7",
    "signature": "\"(*, transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', riverhog_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], capability_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetRuntimeAuthority",
  "unit": "export"
}
```
