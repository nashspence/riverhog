# riverhog_client.RawSourceHash.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-rawsourcehash-close:96a6b4228e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a6e58267a4"></a>
- <a id="s-eabb8784a4"></a>`distribution`: `riverhog-client`
- <a id="s-29dac34cc3"></a>`module`: `riverhog_client`
- <a id="s-c025296bcd"></a>`name`: `close`
- <a id="s-3cda1751cb"></a>`owner`: `riverhog_client.RawSourceHash`
- <a id="s-f58ee8a38f"></a>`unit`: `member`

### Declared structure

- <a id="s-fe5486375b"></a>`kind`: `"method"`
- <a id="s-4c2f7e0a69"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [RawSourceHash](riverhog-client-rawsourcehash.md)

## Governing policies

- <a id="pa-480fbae1ec"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.RawSourceHash.close`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13a930f0eafe6304599b64c23bd1f9bcb12fcd04ceddb9138a8a49ff377d1fed -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "close",
  "owner": "riverhog_client.RawSourceHash",
  "unit": "member"
}
```

</details>
