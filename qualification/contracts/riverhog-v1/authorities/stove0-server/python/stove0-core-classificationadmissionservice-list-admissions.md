# stove0_core.ClassificationAdmissionService.list_admissions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-classificationadmissionservic-c3a1704819:bac208c3c1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-77c92b57df"></a>
- <a id="s-2f0e669f33"></a>`distribution`: `stove0-server`
- <a id="s-a18c0c5046"></a>`module`: `stove0_core`
- <a id="s-8a504478bb"></a>`name`: `list_admissions`
- <a id="s-c0466ca7f3"></a>`owner`: `stove0_core.ClassificationAdmissionService`
- <a id="s-d4b0fcdcc6"></a>`unit`: `member`

### Declared structure

- <a id="s-03dadb905f"></a>`kind`: `"method"`
- <a id="s-37643df7b5"></a>`signature`: `"\"(self, *, page_size: 'int', position: 'tuple[str \| int \| bool \| bytes \| None, ...] \| None', policy_id: 'str \| None', state: 'AdmissionState \| None', query: 'str \| None', sort: 'AdmissionSort', order: 'SortOrder') -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [ClassificationAdmissionService](stove0-core-classificationadmissionservice.md)

## Governing policies

- <a id="pa-d69d2b1312"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.ClassificationAdmissionService.list_admissions`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b49f2b4057dd52504437c2cd3ac4907ecff3f7d44844aae36fa33ef4f5eaf5a0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int', position: 'tuple[str | int | bool | bytes | None, ...] | None', policy_id: 'str | None', state: 'AdmissionState | None', query: 'str | None', sort: 'AdmissionSort', order: 'SortOrder') -> 'dict[str, object]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "list_admissions",
  "owner": "stove0_core.ClassificationAdmissionService",
  "unit": "member"
}
```

</details>
