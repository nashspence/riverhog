# riverhog_age.ResumableAgeScryptSession.from_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-resumableagescryptsession-from-state:dcb3f10804 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-98c36a2933"></a>
- <a id="s-b00bd83645"></a>`distribution`: `riverhog-age`
- <a id="s-0aeae77edc"></a>`module`: `riverhog_age`
- <a id="s-e63491f34f"></a>`name`: `from_state`
- <a id="s-0067a4abfb"></a>`owner`: `riverhog_age.ResumableAgeScryptSession`
- <a id="s-590bcdcf11"></a>`unit`: `member`

### Declared structure

- <a id="s-72d4d903f9"></a>`kind`: `"classmethod"`
- <a id="s-75590a15bd"></a>`signature`: `"\"(cls, passphrase: 'str \| bytes', state: 'UploadState \| bytes \| str \| Mapping[str, object]', *, scrypt_maxmem: 'int \| None' = None) -> 'ResumableAgeScryptSession'\""`

## Maintained corroboration

### Related interface records

- [ResumableAgeScryptSession](riverhog-age-resumableagescryptsession.md)

## Governing policies

- <a id="pa-0a58d82433"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-age:riverhog_age](../../../evidence/sources/authorities.md#src-a842e50b8b) — [packages/riverhog-age/src/riverhog\_age/\_\_init\_\_.py](../../../../../../packages/riverhog-age/src/riverhog_age/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_age.ResumableAgeScryptSession.from_state`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
