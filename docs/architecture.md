# Architecture

Contracts define packages; architecture records ownership.

## Authority model

- **Archive authority.** Archive stores own encrypted bytes. Only sealed objects and published
  immutable roots are archive authority; caches rebuild. Retirement/deletion mutate archives; archives remain
  recoverable with standard tools without Riverhog's service or database.
- **Trust boundary.** Riverhog's host is the plaintext/encryption boundary. Ingress encrypts there;
  storage adapters receive ciphertext units. Authorized clients/workers own plaintext custody.
  Ingress is not a storage tier.
  Collections freeze their encryption format and opaque key identity; configuration owns keys.
- **Operational state.** Database state records identity, placement, and workflows.
  Checkpoints, unsealed membership, and open resumable writes are not archive authority.
- **Provenance authority.** Per-file provenance is append-only custody history. Journals remain
  exact prefixes across handoffs; clients capture or continue them by default, and omissions require
  a reason. Journals and their index are authoritative; database rows are a rebuildable projection.
- **Collection views.** Collections are immutable namespaces/deletion units. Mutable descriptions
  and classification-tag sets are copy-adjacent recovery material projected into the catalog;
  tag membership may label authorization; applications own richer indexes.
- **Deployment configuration.** Deployments own credentials, topology, and policies.

## Boundary model

- **Implementation ownership.** Riverhog owns product implementations and generic contracts.
  Reference applications own their workflows and public interfaces; reference components own one
  independently selected extension capability. Both are nonnormative, family-owned, and enter
  Riverhog only through public contracts.
- **Public contracts.** Published HTTP and CloudEvents contracts define integration. Shared models
  own identities; HTTP/OpenAPI owns CRUD and maintained-client JSON. The generated freeze
  inventories external extents.
- **Riverhog platform.** Server owns archives, transfer, retrieval, and
  delivery. Generic client owns transport, production, sync, and capability-scoped processing;
  applications own interfaces and materializations.
- **Ingress adapters.** Ingress adapters are content-opaque, bounded-custody Riverhog clients; they
  relinquish bytes only after finalization.
- **Storage adapters.** Storage adapters translate opaque objects into provider mechanisms and
  materialize canonical logical trees; provider policy remains external.
- **Reference applications.** Reference applications use Riverhog capabilities and own their state;
  their contracts are authoritative for the application, not for Riverhog.
- **Extensions.** Observers report immutable-artifact facts; targets perform declared operations.
  Each selected distribution owns one capability; shared-dependency image bundles preserve separate
  identities and selection. Names identify families; only exact digest-bound contracts or selected
  bindings carry authority.
- **Transfer path.** Payload loops exclude control-plane status and reporting work.

Workspace checks enforce ownership boundaries.

## Repository map

- [`riverhog`](../riverhog/): archive service.
- [`reference/gogurt`](../reference/gogurt/): Gogurt reference application and components.
- [`reference/riverhog`](../reference/riverhog/): Riverhog reference applications and components,
  including the permissively licensed independent recovery tool.
- [`reference/stove0`](../reference/stove0/): Stove0 reference application and components.
- [`packages`](../packages/): product-owned contracts and support.
