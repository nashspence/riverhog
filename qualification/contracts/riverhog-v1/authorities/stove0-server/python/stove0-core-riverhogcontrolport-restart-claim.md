# stove0_core.RiverhogControlPort.restart_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogcontrolport-restart-claim:a5a00cc72c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3864bef409"></a>
- <a id="s-51ab1300bf"></a>`distribution`: `stove0-server`
- <a id="s-5d1eaf4bef"></a>`module`: `stove0_core`
- <a id="s-c47ebda34a"></a>`name`: `restart_claim`
- <a id="s-45efee762d"></a>`owner`: `stove0_core.RiverhogControlPort`
- <a id="s-054bc4b1d3"></a>`unit`: `member`

### Declared structure

- <a id="s-b328a48e05"></a>`kind`: `"method"`
- <a id="s-926688425e"></a>`signature`: `"\"(self, work: 'WorkIdentity', claim: 'ClaimBinding') -> 'ClaimBinding'\""`

## Maintained corroboration

### Related interface records

- [RiverhogControlPort](stove0-core-riverhogcontrolport.md)

## Governing policies

- <a id="pa-670265859f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogControlPort.restart_claim`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 783446fc53a2ea6f70699cddd6d502065a9c5d2ee242249b2e1633597c7f45ac -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work: 'WorkIdentity', claim: 'ClaimBinding') -> 'ClaimBinding'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "restart_claim",
  "owner": "stove0_core.RiverhogControlPort",
  "unit": "member"
}
```

</details>
