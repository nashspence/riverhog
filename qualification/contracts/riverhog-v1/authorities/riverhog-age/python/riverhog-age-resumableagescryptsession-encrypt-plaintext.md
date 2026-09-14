# riverhog_age.ResumableAgeScryptSession.encrypt_plaintext

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-resumableagescryptsession-en-1a45e86c65:18ee158a61 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3eb25d0364"></a>
- <a id="s-8154bad548"></a>`distribution`: `riverhog-age`
- <a id="s-e5c2348340"></a>`module`: `riverhog_age`
- <a id="s-413534ab6f"></a>`name`: `encrypt_plaintext`
- <a id="s-9beb3f5c94"></a>`owner`: `riverhog_age.ResumableAgeScryptSession`
- <a id="s-6c23f4f833"></a>`unit`: `member`

### Declared structure

- <a id="s-e3be2e840f"></a>`kind`: `"method"`
- <a id="s-1f081f4f19"></a>`signature`: `"\"(self, plaintext: 'bytes') -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [ResumableAgeScryptSession](riverhog-age-resumableagescryptsession.md)

## Governing policies

- <a id="pa-a44fc56d06"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — `packages/riverhog-age/src/riverhog_age/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_age.ResumableAgeScryptSession.encrypt_plaintext`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c9ae928d9e74aa306a78ed4f93fa7d60ecb2a1dbd87f9da633e14cb1696a2ef7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, plaintext: 'bytes') -> 'bytes'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "encrypt_plaintext",
  "owner": "riverhog_age.ResumableAgeScryptSession",
  "unit": "member"
}
```
