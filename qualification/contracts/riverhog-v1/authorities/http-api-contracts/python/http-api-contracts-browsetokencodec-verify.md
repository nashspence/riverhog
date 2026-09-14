# http_api_contracts.BrowseTokenCodec.verify

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-browsetokencodec-verify:a84983f28e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dae443d143"></a>
- <a id="s-ca1507025a"></a>`distribution`: `http-api-contracts`
- <a id="s-270c26a9da"></a>`module`: `http_api_contracts`
- <a id="s-602582e43c"></a>`name`: `verify`
- <a id="s-c706070041"></a>`owner`: `http_api_contracts.BrowseTokenCodec`
- <a id="s-a7c42fb8aa"></a>`unit`: `member`

### Declared structure

- <a id="s-1481f1ea31"></a>`kind`: `"method"`
- <a id="s-512bff1531"></a>`signature`: `"\"(self, token: 'str \| None', *, operation: 'str', principal: 'object', selectors: 'Mapping[str, object]') -> 'tuple[BrowseScalar, ...] \| None'\""`

## Maintained corroboration

### Related interface records

- [http_api_contracts.BrowseTokenCodec](http-api-contracts-browsetokencodec.md)

## Governing policies

- <a id="pa-0ab6d46639"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.BrowseTokenCodec.verify`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 695e2220de60810c4e87773c5e3fd20d6af08bd87bb17d1a64bb7b5f278507b8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, token: 'str | None', *, operation: 'str', principal: 'object', selectors: 'Mapping[str, object]') -> 'tuple[BrowseScalar, ...] | None'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "verify",
  "owner": "http_api_contracts.BrowseTokenCodec",
  "unit": "member"
}
```
