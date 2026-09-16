# stove0_core.ClassificationAdmissionService.advance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-classificationadmissionservice-advance:a8750cd3fe -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e69054c07f"></a>
- <a id="s-008fa65ca3"></a>`distribution`: `stove0-server`
- <a id="s-8f31bb2b37"></a>`module`: `stove0_core`
- <a id="s-0751f21cfc"></a>`name`: `advance`
- <a id="s-0a69bd50cd"></a>`owner`: `stove0_core.ClassificationAdmissionService`
- <a id="s-d88c591458"></a>`unit`: `member`

### Declared structure

- <a id="s-ffeb89f348"></a>`kind`: `"method"`
- <a id="s-66e133e765"></a>`signature`: `"\"(self, *, limit: 'int' = 25) -> 'AdmissionRun'\""`

## Maintained corroboration

### Related interface records

- [ClassificationAdmissionService](stove0-core-classificationadmissionservice.md)

## Governing policies

- <a id="pa-4509ab611d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.ClassificationAdmissionService.advance`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 482569754e538eecf05de4eda020dfd0611096c2827c0706db9ed9a988d7f3bb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, limit: 'int' = 25) -> 'AdmissionRun'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "advance",
  "owner": "stove0_core.ClassificationAdmissionService",
  "unit": "member"
}
```

</details>
