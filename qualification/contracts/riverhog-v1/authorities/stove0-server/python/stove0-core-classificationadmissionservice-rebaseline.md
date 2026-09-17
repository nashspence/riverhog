# stove0_core.ClassificationAdmissionService.rebaseline

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-classificationadmissionservic-2d69e13c5f:15825397a4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6cf4ea1451"></a>
- <a id="s-ea67479dc7"></a>`distribution`: `stove0-server`
- <a id="s-e2f698244e"></a>`module`: `stove0_core`
- <a id="s-85662dd33d"></a>`name`: `rebaseline`
- <a id="s-871614e04e"></a>`owner`: `stove0_core.ClassificationAdmissionService`
- <a id="s-f57563e823"></a>`unit`: `member`

### Declared structure

- <a id="s-c795b09e66"></a>`kind`: `"method"`
- <a id="s-cc6e060e33"></a>`signature`: `"'(self, policy_id: \\'str\\', *, mode: \"Literal[\\'observe\\', \\'backfill\\']\" = \\'observe\\') -> \\'AdmissionPolicyStatus\\''"`

## Maintained corroboration

### Related interface records

- [ClassificationAdmissionService](stove0-core-classificationadmissionservice.md)

## Governing policies

- <a id="pa-cf41fbfeb8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.ClassificationAdmissionService.rebaseline`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 69db8a10a80f17911be969012945b90a4b359bea25d1cd1cb1562610a4a3d311 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "'(self, policy_id: \\'str\\', *, mode: \"Literal[\\'observe\\', \\'backfill\\']\" = \\'observe\\') -> \\'AdmissionPolicyStatus\\''"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "rebaseline",
  "owner": "stove0_core.ClassificationAdmissionService",
  "unit": "member"
}
```

</details>
