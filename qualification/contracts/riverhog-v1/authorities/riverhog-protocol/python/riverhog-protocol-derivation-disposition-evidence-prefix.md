# riverhog_protocol.DERIVATION_DISPOSITION_EVIDENCE_PREFIX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-derivation-disposition-adeba4ea99:4b0694aafc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-63e3961f43"></a>
- <a id="s-f6e2a31125"></a>`distribution`: `riverhog-protocol`
- <a id="s-32e0558b1b"></a>`module`: `riverhog_protocol`
- <a id="s-b1b4fbe319"></a>`name`: `DERIVATION_DISPOSITION_EVIDENCE_PREFIX`
- <a id="s-703ce541f1"></a>`unit`: `export`

### Declared structure

- <a id="s-d33f59548c"></a>`kind`: `"constant"`
- <a id="s-65769aca2a"></a>`value`: `"riverhog/derivation/dispositions"`

## Governing policies

- <a id="pa-213211af55"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.DERIVATION_DISPOSITION_EVIDENCE_PREFIX`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7130739377417bc2dacf538b980d15e342a846076f582198dd40328dcf4ad888 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog/derivation/dispositions"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "DERIVATION_DISPOSITION_EVIDENCE_PREFIX",
  "unit": "export"
}
```

</details>
