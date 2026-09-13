# riverhog_age.UploadState.to_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-uploadstate-to-json-bytes:ff52661d4e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2a73ff8de5"></a>
| Field | Shape |
|---|---|
| <a id="s-2050bd4665"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ef71bec6c3"></a>`distribution` | "riverhog-age" |
| <a id="s-eae170f6e3"></a>`module` | "riverhog_age" |
| <a id="s-7ef14d5915"></a>`name` | "to_json_bytes" |
| <a id="s-adfeab19f7"></a>`owner` | "riverhog_age.UploadState" |
| <a id="s-3d42360aa3"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_age.UploadState](riverhog-age-uploadstate.md)

## Governing policies

- <a id="pa-7d14514155"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — `packages/riverhog-age/src/riverhog_age/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_age.UploadState.to_json_bytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b25656edafea73b5f13f02cf9cffcc224e75f16e45801d7d2ca7ecb5271d0950 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bytes'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "to_json_bytes",
  "owner": "riverhog_age.UploadState",
  "unit": "member"
}
```
