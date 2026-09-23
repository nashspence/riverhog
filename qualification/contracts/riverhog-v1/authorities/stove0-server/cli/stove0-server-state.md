# stove0-server state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-server:stove0-server-state:fefcdf0037 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-3143e65bdd"></a>Parser name: `state`

| Field | Value |
|---|---|
| <a id="s-8161b95542"></a>`parameters` | `[]` |
- <a id="s-18150d219c"></a>Subcommand selection: required.
- <a id="s-86b49ad6aa"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-6cdc4ea8e5"></a>`help` | <a id="s-ddfdd7c0dd"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-f4100d02a9"></a>`0` | <a id="s-d3d1c60aa1"></a>`"noncontractual-framework-help"` | <a id="s-2ff2f4073b"></a>`"empty"` |

## Governing policies

- <a id="pa-f10e91e57c"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-server](../../../evidence/sources/authorities.md#src-6f5bc9f6db) — [some-implementations/stove0/application/server/src/stove0\_api/app.py::&lt;module&gt;](../../../../../../some-implementations/stove0/application/server/src/stove0_api/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0-server/commands/state/allow_abbrev`
- `/external_contract/cli/stove0-server/commands/state/name`
- `/external_contract/cli/stove0-server/commands/state/parameters`
- `/external_contract/cli/stove0-server/commands/state/subcommand_required`
- `/external_contract/cli/stove0-server/commands/state/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-server/commands/state/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-server/commands/state/name`

<!-- exact-contract-value: 55af5b1d18b0d9ddda33273d987fd0579ef9b07af3a077c79d0accb6c8f17127 -->

```json
"state"
```

### `/external_contract/cli/stove0-server/commands/state/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0-server/commands/state/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-server/commands/state/terminating_controls`

<!-- exact-contract-value: 46c96c22d2ed8a51da57bba3ac0f2269bb35f98c5fd6dc3e3ba67e60f772ad72 -->

```json
[
  {
    "exit_status": 0,
    "id": "help",
    "stderr": "empty",
    "stdout": "noncontractual-framework-help",
    "trigger": {
      "kind": "option-present",
      "options": [
        "-h",
        "--help"
      ]
    }
  }
]
```

</details>
