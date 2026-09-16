# riverhog_client.ApiClient.__exit__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-exit:2c9fb7add5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ee48af0797"></a>
- <a id="s-bc9679e645"></a>`distribution`: `riverhog-client`
- <a id="s-209319387b"></a>`module`: `riverhog_client`
- <a id="s-29256aa368"></a>`name`: `__exit__`
- <a id="s-4905b0a055"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-19f93063e3"></a>`unit`: `member`

### Declared structure

- <a id="s-d39d2f7a80"></a>`kind`: `"method"`
- <a id="s-f440e46ba6"></a>`signature`: `"\"(self, *_: 'object') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-b6dbb8367e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.__exit__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dec9598619827831973996530acb0a0c2147e369eba06a4c62bc4d27d2cbbf03 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *_: 'object') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "__exit__",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
