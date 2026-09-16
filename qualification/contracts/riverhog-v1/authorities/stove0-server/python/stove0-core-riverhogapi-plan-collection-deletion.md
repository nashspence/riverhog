# stove0_core.RiverhogApi.plan_collection_deletion

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-plan-collection-deletion:8a07a0ff2e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e56c2a6b19"></a>
- <a id="s-5126702176"></a>`distribution`: `stove0-server`
- <a id="s-78fc8cdaa6"></a>`module`: `stove0_core`
- <a id="s-5a7ccc6c98"></a>`name`: `plan_collection_deletion`
- <a id="s-1161370d9a"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-61599e18ff"></a>`unit`: `member`

### Declared structure

- <a id="s-3220dcc7c2"></a>`kind`: `"method"`
- <a id="s-be67df50b2"></a>`signature`: `"\"(self, collection_id: 'int', *, retirement_claim_id: 'str \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-1dd36ebad0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.plan_collection_deletion`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e44df0d4d428879799a72070aa3a1fa199bf29cec41dc915931c762010c7c817 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'int', *, retirement_claim_id: 'str | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "plan_collection_deletion",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```

</details>
