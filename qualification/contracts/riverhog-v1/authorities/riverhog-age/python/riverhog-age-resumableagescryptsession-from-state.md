# riverhog_age.ResumableAgeScryptSession.from_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-resumableagescryptsession-from-state:dcb3f10804 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-98c36a2933"></a>
| Field | Shape |
|---|---|
| <a id="s-57cd69ae07"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-b00bd83645"></a>`distribution` | "riverhog-age" |
| <a id="s-0aeae77edc"></a>`module` | "riverhog_age" |
| <a id="s-e63491f34f"></a>`name` | "from_state" |
| <a id="s-0067a4abfb"></a>`owner` | "riverhog_age.ResumableAgeScryptSession" |
| <a id="s-590bcdcf11"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_age.ResumableAgeScryptSession](riverhog-age-resumableagescryptsession.md)

## Governing policies

- <a id="pa-0a58d82433"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — `packages/riverhog-age/src/riverhog_age/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_age.ResumableAgeScryptSession.from_state`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 041341009620a5a3efbd1491fe75d3d2056f4b5faa15105d43fbff237d04d099 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, passphrase: 'str | bytes', state: 'UploadState | bytes | str | Mapping[str, object]', *, scrypt_maxmem: 'int | None' = None) -> 'ResumableAgeScryptSession'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "from_state",
  "owner": "riverhog_age.ResumableAgeScryptSession",
  "unit": "member"
}
```
