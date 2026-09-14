# riverhog_age.ResumableAgeScryptSession.encrypt_part

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-resumableagescryptsession-encrypt-part:ff445db6d1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c8b95bd532"></a>
- <a id="s-9b2e9b4206"></a>`distribution`: `riverhog-age`
- <a id="s-35207d38c2"></a>`module`: `riverhog_age`
- <a id="s-df92abf75e"></a>`name`: `encrypt_part`
- <a id="s-795a97e56f"></a>`owner`: `riverhog_age.ResumableAgeScryptSession`
- <a id="s-5ee562fa70"></a>`unit`: `member`

### Declared structure

- <a id="s-3e7b9bf650"></a>`kind`: `"method"`
- <a id="s-99c3c7892a"></a>`signature`: `"\"(self, plan: 'AgeAlignedUnitPlan', plaintext_chunk_provider: 'Callable[[int, int, int], bytes]', *, plaintext_size: 'int') -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [ResumableAgeScryptSession](riverhog-age-resumableagescryptsession.md)

## Governing policies

- <a id="pa-1f4e5a632b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — `packages/riverhog-age/src/riverhog_age/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_age.ResumableAgeScryptSession.encrypt_part`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c11acac1ae365637c2275a781a08a57ac5053e8fe290d9fdac1064f4a841886e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, plan: 'AgeAlignedUnitPlan', plaintext_chunk_provider: 'Callable[[int, int, int], bytes]', *, plaintext_size: 'int') -> 'bytes'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "encrypt_part",
  "owner": "riverhog_age.ResumableAgeScryptSession",
  "unit": "member"
}
```
