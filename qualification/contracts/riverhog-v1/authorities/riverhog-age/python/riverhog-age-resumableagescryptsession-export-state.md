# riverhog_age.ResumableAgeScryptSession.export_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-resumableagescryptsession-export-state:5321148b07 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-22064c479b"></a>
- <a id="s-f57195dd69"></a>`distribution`: `riverhog-age`
- <a id="s-ded0548243"></a>`module`: `riverhog_age`
- <a id="s-076dc21f92"></a>`name`: `export_state`
- <a id="s-87fcbbaf36"></a>`owner`: `riverhog_age.ResumableAgeScryptSession`
- <a id="s-c81f6246aa"></a>`unit`: `member`

### Declared structure

- <a id="s-585dd43642"></a>`kind`: `"method"`
- <a id="s-a9b7e4ad29"></a>`signature`: `"\"(self, *, plaintext_size: 'int \| None' = None) -> 'UploadState'\""`

## Maintained corroboration

### Related interface records

- [ResumableAgeScryptSession](riverhog-age-resumableagescryptsession.md)

## Governing policies

- <a id="pa-2f9a9fa9b5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — `packages/riverhog-age/src/riverhog_age/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_age.ResumableAgeScryptSession.export_state`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bb8ab3f0f6b6508b0f9baf5e9091a09b976918b99c78d0d2fe833d136e830da4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, plaintext_size: 'int | None' = None) -> 'UploadState'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "export_state",
  "owner": "riverhog_age.ResumableAgeScryptSession",
  "unit": "member"
}
```
