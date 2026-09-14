# http_api_contracts.HttpOperationErrorAuthority.from_codes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-httpoperationerrorauth-33c9f16761:65afd62aed -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-94a612992f"></a>
- <a id="s-6294812e46"></a>`distribution`: `http-api-contracts`
- <a id="s-149e2b5aef"></a>`module`: `http_api_contracts`
- <a id="s-6ec9466642"></a>`name`: `from_codes`
- <a id="s-a590c13ddc"></a>`owner`: `http_api_contracts.HttpOperationErrorAuthority`
- <a id="s-bb4821831b"></a>`unit`: `member`

### Declared structure

- <a id="s-aaa40fdb46"></a>`kind`: `"classmethod"`
- <a id="s-d23bf5a9e7"></a>`signature`: `"\"(cls, *, common: 'Collection[str]', operation: 'Mapping[str, Collection[str]]', exact: 'Mapping[str, Collection[str]] \| None' = None) -> 'HttpOperationErrorAuthority'\""`

## Maintained corroboration

### Related interface records

- [HttpOperationErrorAuthority](http-api-contracts-httpoperationerrorauthority.md)

## Governing policies

- <a id="pa-86cdc76265"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.HttpOperationErrorAuthority.from_codes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3e467d8faa62caaf7b6317f7d22eb760b2fc1ef0b4e1e32f781a3bec65151681 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, common: 'Collection[str]', operation: 'Mapping[str, Collection[str]]', exact: 'Mapping[str, Collection[str]] | None' = None) -> 'HttpOperationErrorAuthority'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "from_codes",
  "owner": "http_api_contracts.HttpOperationErrorAuthority",
  "unit": "member"
}
```
