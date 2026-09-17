# riverhog_application_access.ApplicationAccessGrant.as_access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-applicationac-7f3711ec69:1539010bd7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ad9f2e55de"></a>
- <a id="s-0367037543"></a>`distribution`: `riverhog-application-access`
- <a id="s-4c41186cb3"></a>`module`: `riverhog_application_access`
- <a id="s-fcc7fb575c"></a>`name`: `as_access`
- <a id="s-6a6804902e"></a>`owner`: `riverhog_application_access.ApplicationAccessGrant`
- <a id="s-ab2b3a35ca"></a>`unit`: `member`

### Declared structure

- <a id="s-e7e1df78bf"></a>`kind`: `"method"`
- <a id="s-d633a09d32"></a>`signature`: `"\"(self) -> 'ApplicationAccess'\""`

## Maintained corroboration

### Related interface records

- [ApplicationAccessGrant](riverhog-application-access-applicationaccessgrant.md)

## Governing policies

- <a id="pa-42c420d4d1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources/authorities.md#src-9d9ce5fdac) — [packages/riverhog-application-access/src/riverhog\_application\_access/\_\_init\_\_.py](../../../../../../packages/riverhog-application-access/src/riverhog_application_access/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_application_access.ApplicationAccessGrant.as_access`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 22f60e898e27cfadb833b6bdccd945c8934577dd29b279c0d4f4978aa0bc8630 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'ApplicationAccess'\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "as_access",
  "owner": "riverhog_application_access.ApplicationAccessGrant",
  "unit": "member"
}
```

</details>
