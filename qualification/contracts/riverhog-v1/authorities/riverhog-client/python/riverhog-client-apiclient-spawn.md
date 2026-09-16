# riverhog_client.ApiClient.spawn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-spawn:f42dc50078 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-70501732c8"></a>
- <a id="s-f138246575"></a>`distribution`: `riverhog-client`
- <a id="s-e904ecbc47"></a>`module`: `riverhog_client`
- <a id="s-2bf3ec3ca7"></a>`name`: `spawn`
- <a id="s-1d3f8f4117"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-b70a5ab1cd"></a>`unit`: `member`

### Declared structure

- <a id="s-2257c835db"></a>`kind`: `"method"`
- <a id="s-e086658df6"></a>`signature`: `"\"(self) -> 'ApiClient'\""`

## Maintained corroboration

### Related interface records

- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-1b317e9500"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.spawn`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c587202451580f2dbec888a8bffe25a6c0b71742fb8f031a17452b5ee2dc3a5e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'ApiClient'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "spawn",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
