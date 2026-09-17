# riverhog_protocol.RiverhogEventPage.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-riverhogeventpage-get:df7f7fa71f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a55cfec7e6"></a>
- <a id="s-b75b42bf14"></a>`distribution`: `riverhog-protocol`
- <a id="s-0e63b38ef3"></a>`module`: `riverhog_protocol`
- <a id="s-9c2512caba"></a>`name`: `get`
- <a id="s-079f477752"></a>`owner`: `riverhog_protocol.RiverhogEventPage`
- <a id="s-37a441054d"></a>`unit`: `member`

### Declared structure

- <a id="s-89f3947f64"></a>`kind`: `"method"`
- <a id="s-13f1979754"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [RiverhogEventPage](riverhog-protocol-riverhogeventpage.md)

## Governing policies

- <a id="pa-a7689263a2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.RiverhogEventPage.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6d23009dbb7c60ecadadbeece76fe26ae9505a27d7899ad54ab668f6d9cf7ffe -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.RiverhogEventPage",
  "unit": "member"
}
```

</details>
