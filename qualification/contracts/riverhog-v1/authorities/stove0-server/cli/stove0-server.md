# stove0-server

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-server:stove0-server:a568d7d141 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-031448ef76"></a>Parser name: `stove0-server`

| Field | Value |
|---|---|
| <a id="s-ebf3a6dffb"></a>`parameters` | `[]` |
- <a id="s-5d6b18e27a"></a>Subcommand selection: required.
- <a id="s-76b2303574"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-ff45613782"></a>`help` | <a id="s-0ea3fd9720"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-b56b68ec23"></a>`0` | <a id="s-c08d675bd8"></a>`"noncontractual-framework-help"` | <a id="s-5e318afb1a"></a>`"empty"` |
| <a id="s-bdc50659a1"></a>`version` | <a id="s-64be0be354"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-ee5b2f8b22"></a>`0` | <a id="s-272d904e84"></a>`{"distribution":"stove0-server","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-95a7238f3b"></a>`"empty"` |

## Governing policies

- <a id="pa-03bffe565f"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-server](../../../evidence/sources/authorities.md#src-6f5bc9f6db) — [some-implementations/stove0/application/server/src/stove0\_api/app.py::&lt;module&gt;](../../../../../../some-implementations/stove0/application/server/src/stove0_api/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0-server/allow_abbrev`
- `/external_contract/cli/stove0-server/name`
- `/external_contract/cli/stove0-server/parameters`
- `/external_contract/cli/stove0-server/subcommand_required`
- `/external_contract/cli/stove0-server/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-server/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-server/name`

<!-- exact-contract-value: 8d975b7503c749fb11fdc973a2bb3d1063a032fa5498cadc746fbf109812d14c -->

```json
"stove0-server"
```

### `/external_contract/cli/stove0-server/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0-server/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-server/terminating_controls`

<!-- exact-contract-value: 1d70edbae1d654fb0a654229981056d587170205aae96b2e1a36cc340f621aeb -->

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
  },
  {
    "exit_status": 0,
    "id": "version",
    "stderr": "empty",
    "stdout": {
      "distribution": "stove0-server",
      "kind": "installed-coordinated-release-version",
      "serialization": "noncontractual"
    },
    "trigger": {
      "kind": "option-present",
      "options": [
        "--version"
      ]
    }
  }
]
```

</details>
