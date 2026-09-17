# riverhog_protocol.PortableCollectionIdentityBuilder

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-portablecollectionidentitybuilder:9a910a2e1c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-55f887256a"></a>
- <a id="s-95a26113d3"></a>`distribution`: `riverhog-protocol`
- <a id="s-12f23a2666"></a>`module`: `riverhog_protocol`
- <a id="s-4296b6a8ee"></a>`name`: `PortableCollectionIdentityBuilder`
- <a id="s-8bf873c5db"></a>`unit`: `export`

### Declared structure

- <a id="s-8eb7d3fb77"></a>`kind`: `"class"`
- <a id="s-a3a55742e7"></a>`signature`: `"\"(header: 'PortableCollectionHeader') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [add](riverhog-protocol-portablecollectionidentitybuilder-add.md)
- [identity](riverhog-protocol-portablecollectionidentitybuilder-identity.md)

## Governing policies

- <a id="pa-314a3cacef"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.PortableCollectionIdentityBuilder`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a3c8d612069c4027903714edba9f5e0e760d01bbbf7162bd9c422ce6ca9016c0 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(header: 'PortableCollectionHeader') -> 'None'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "PortableCollectionIdentityBuilder",
  "unit": "export"
}
```

</details>
