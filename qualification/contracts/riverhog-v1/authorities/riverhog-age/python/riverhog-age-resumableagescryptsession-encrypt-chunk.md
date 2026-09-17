# riverhog_age.ResumableAgeScryptSession.encrypt_chunk

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-resumableagescryptsession-encrypt-chunk:a21232e221 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4c1d2e044b"></a>
- <a id="s-b9fc2df568"></a>`distribution`: `riverhog-age`
- <a id="s-dcbcee79bd"></a>`module`: `riverhog_age`
- <a id="s-9e2a9c500c"></a>`name`: `encrypt_chunk`
- <a id="s-5f1958fec4"></a>`owner`: `riverhog_age.ResumableAgeScryptSession`
- <a id="s-ce78a74acd"></a>`unit`: `member`

### Declared structure

- <a id="s-325586bcaa"></a>`kind`: `"method"`
- <a id="s-1a83dbc5d6"></a>`signature`: `"\"(self, chunk_index: 'int', plaintext: 'bytes', *, final: 'bool') -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [ResumableAgeScryptSession](riverhog-age-resumableagescryptsession.md)

## Governing policies

- <a id="pa-0b8483ce59"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-age:riverhog_age](../../../evidence/sources/authorities.md#src-a842e50b8b) — [packages/riverhog-age/src/riverhog\_age/\_\_init\_\_.py](../../../../../../packages/riverhog-age/src/riverhog_age/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_age.ResumableAgeScryptSession.encrypt_chunk`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 85bd8df269dd0acb06672ab9da2f5b3d08d8ada46189328d5b5731d78174fe24 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, chunk_index: 'int', plaintext: 'bytes', *, final: 'bool') -> 'bytes'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "encrypt_chunk",
  "owner": "riverhog_age.ResumableAgeScryptSession",
  "unit": "member"
}
```

</details>
