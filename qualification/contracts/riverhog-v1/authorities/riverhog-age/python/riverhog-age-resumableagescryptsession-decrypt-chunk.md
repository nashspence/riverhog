# riverhog_age.ResumableAgeScryptSession.decrypt_chunk

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-resumableagescryptsession-decrypt-chunk:8fb3d824e0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-efcad74a6b"></a>
| Field | Shape |
|---|---|
| <a id="s-ed0062784f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-92b034c9ea"></a>`distribution` | "riverhog-age" |
| <a id="s-7656d1215b"></a>`module` | "riverhog_age" |
| <a id="s-36783c838e"></a>`name` | "decrypt_chunk" |
| <a id="s-7a5fd6c357"></a>`owner` | "riverhog_age.ResumableAgeScryptSession" |
| <a id="s-216c96c3e2"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_age.ResumableAgeScryptSession](riverhog-age-resumableagescryptsession.md)

## Governing policies

- <a id="pa-4770f23533"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — `packages/riverhog-age/src/riverhog_age/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_age.ResumableAgeScryptSession.decrypt_chunk`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6f00b8c3cd2c6e3f52964c2c8c59f14d67cc9acf28670388fc6d00195c7e79db -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, chunk_index: 'int', ciphertext: 'bytes', *, final: 'bool') -> 'bytes'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "decrypt_chunk",
  "owner": "riverhog_age.ResumableAgeScryptSession",
  "unit": "member"
}
```
