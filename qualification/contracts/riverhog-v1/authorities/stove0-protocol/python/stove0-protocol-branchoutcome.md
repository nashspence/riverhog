# stove0_protocol.BranchOutcome

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchoutcome:204ae48507 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ed0e6bd90e"></a>
| Field | Shape |
|---|---|
| <a id="s-6aaa423564"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-fd41d0605d"></a>`distribution` | "stove0-protocol" |
| <a id="s-83d48ae6c4"></a>`module` | "stove0_protocol" |
| <a id="s-ec5a5e650d"></a>`name` | "BranchOutcome" |
| <a id="s-5ad96785e6"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.BranchOutcome.exact_declared_plan](stove0-protocol-branchoutcome-exact-declared-plan.md)

## Governing policies

- <a id="pa-ac99b95459"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BranchOutcome`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 271a434ff1ed4e31288f0ef8063a34f452a47033ec2de0463002680c5f206a32 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "994be4a7a6a634822af2aeca476070657d99b942dad13078f6d8aa70d683ad3e",
    "signature": "\"(*, format: Literal['stove0-branch-outcome/v1'] = 'stove0-branch-outcome/v1', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, branch_set_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, state: Literal['failed', 'inapplicable', 'interrupted', 'canceled']) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BranchOutcome",
  "unit": "export"
}
```
