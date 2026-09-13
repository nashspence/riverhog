# stove0_core.WorkStore.iter_selection_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-iter-selection-artifacts:1c1f97466c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6f0aad756d"></a>
| Field | Shape |
|---|---|
| <a id="s-7dfd8e8744"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-25377617c4"></a>`distribution` | "stove0-server" |
| <a id="s-fc7198dad6"></a>`module` | "stove0_core" |
| <a id="s-f00194278a"></a>`name` | "iter_selection_artifacts" |
| <a id="s-ff224060c2"></a>`owner` | "stove0_core.WorkStore" |
| <a id="s-512f97f37e"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-53d2378636"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.iter_selection_artifacts`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 239db88554fe86c2cdb336dadfd2f0ec671c17f7e079bc62b9db94d904c4f2a9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection_sha256: 'str') -> 'Iterator[ArtifactSubject]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_selection_artifacts",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```
