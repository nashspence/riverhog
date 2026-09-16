# riverhog_provenance_linux_contracts.PLATFORM_FAMILY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-linux-contracts:riverhog-provenance-linux-contracts-platform-family:ab8103d536 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-linux-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1f8b6bd2d9"></a>
- <a id="s-3c0b79f8fb"></a>`distribution`: `riverhog-provenance-linux-contracts`
- <a id="s-3e2fff5d8f"></a>`module`: `riverhog_provenance_linux_contracts`
- <a id="s-7acb310c11"></a>`name`: `PLATFORM_FAMILY`
- <a id="s-8cf7fc77be"></a>`unit`: `export`

### Declared structure

- <a id="s-ad6adad7d0"></a>`kind`: `"constant"`
- <a id="s-2a599753e7"></a>`value`: `"linux"`

## Governing policies

- <a id="pa-669502294d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-linux-contracts:riverhog_provenance_linux_contracts](../../../evidence/sources.md#src-2cb2292124) — `reference/riverhog/provenance/contracts/linux/src/riverhog_provenance_linux_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance_linux_contracts.PLATFORM_FAMILY`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c12bb24e9640d7eb30f058c4aa5e409723ab8630cc37460573ed67a0d724dfa8 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "linux"
  },
  "distribution": "riverhog-provenance-linux-contracts",
  "module": "riverhog_provenance_linux_contracts",
  "name": "PLATFORM_FAMILY",
  "unit": "export"
}
```

</details>
