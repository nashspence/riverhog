# riverhog_protocol.ProcessingClaimCreateDocument.validate_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimcreatedo-659f55cdbb:7bfc88ada4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0af5ec798b"></a>
- <a id="s-ca9dcf91d8"></a>`distribution`: `riverhog-protocol`
- <a id="s-6851a03b51"></a>`module`: `riverhog_protocol`
- <a id="s-95ed9755cd"></a>`name`: `validate_claim`
- <a id="s-4b147393e0"></a>`owner`: `riverhog_protocol.ProcessingClaimCreateDocument`
- <a id="s-84b346d2f9"></a>`unit`: `member`

### Declared structure

- <a id="s-0bb0740d30"></a>`kind`: `"method"`
- <a id="s-8db7add435"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ProcessingClaimCreateDocument](riverhog-protocol-processingclaimcreatedocument.md)

## Governing policies

- <a id="pa-1394a093d0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimCreateDocument.validate_claim`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 48af1794cfc281418669eb627efff74f6cce5e4176cae8acea75ef4c68ea44b2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_claim",
  "owner": "riverhog_protocol.ProcessingClaimCreateDocument",
  "unit": "member"
}
```

</details>
