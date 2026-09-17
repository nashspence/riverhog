# riverhog_age.ResumableAgeScryptSession.create

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-resumableagescryptsession-create:bf764acf24 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-261f4f0d6d"></a>
- <a id="s-4c1d185be2"></a>`distribution`: `riverhog-age`
- <a id="s-4cd9788b46"></a>`module`: `riverhog_age`
- <a id="s-9ac38c95f0"></a>`name`: `create`
- <a id="s-6bbfeefb23"></a>`owner`: `riverhog_age.ResumableAgeScryptSession`
- <a id="s-1e5b3487e6"></a>`unit`: `member`

### Declared structure

- <a id="s-ac5f7e24e4"></a>`kind`: `"classmethod"`
- <a id="s-234387167e"></a>`signature`: `"\"(cls, passphrase: 'str \| bytes', *, log_n: 'int' = 18, plaintext_size: 'int \| None' = None, file_key: 'bytes \| None' = None, scrypt_salt: 'bytes \| None' = None, payload_nonce: 'bytes \| None' = None, scrypt_maxmem: 'int \| None' = None) -> 'ResumableAgeScryptSession'\""`

## Maintained corroboration

### Related interface records

- [ResumableAgeScryptSession](riverhog-age-resumableagescryptsession.md)

## Governing policies

- <a id="pa-639e544767"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — [packages/riverhog-age/src/riverhog\_age/\_\_init\_\_.py](../../../../../../packages/riverhog-age/src/riverhog_age/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_age.ResumableAgeScryptSession.create`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3ec0fec48fbd9f6792aedbae987b173b273012ae8ecc56942c4959a4e4dc7cc6 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, passphrase: 'str | bytes', *, log_n: 'int' = 18, plaintext_size: 'int | None' = None, file_key: 'bytes | None' = None, scrypt_salt: 'bytes | None' = None, payload_nonce: 'bytes | None' = None, scrypt_maxmem: 'int | None' = None) -> 'ResumableAgeScryptSession'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "create",
  "owner": "riverhog_age.ResumableAgeScryptSession",
  "unit": "member"
}
```

</details>
