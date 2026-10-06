# Architecture

Let’s say someone wants to keep some personal data for decades.

At first, that sounds like a matter of keeping the bytes. In practice, long-term stewardship has less obvious failure modes. Storage fails or becomes too expensive. Providers and software disappear. Encryption creates key-custody problems. Catalogs and indexes can become accidental requirements for recovery. Provenance can become separated from an artifact until perfectly preserved bytes have lost much of their meaning. Systems added for convenience can quietly gain authority they were never meant to have.

Riverhog is designed so that the archive and the meaning preserved with it can outlast the systems used to operate on them.

This document is an architectural orientation. The [published contracts](https://nashspence.github.io/riverhog/) define the precise interfaces and obligations.

## Make the archive the durable thing

Riverhog groups preserved items into *collections*. Each item in a collection is an *artifact*. Riverhog constructs a *canonical archive*: the standard Riverhog archive structure that does not depend on a particular storage provider.

Once a collection is published, its archived contents do not change.

Riverhog treats only *sealed objects*—archive objects it has finished writing—and *published immutable roots*—published archive roots that cannot change—as *archive authority*: the information that defines what has actually been archived.

Partial work, caches, indexes, workflow state, and other operational data do not define archived content. The database records useful information such as identity, placement, and workflow state, but the archive does not depend on that database for its identity.

A *storage adapter* translates between Riverhog’s canonical archive structure and a storage provider’s own storage system. Changing the provider does not change the archive itself.

In short, **only sealed objects and published immutable roots define archived content; the systems around them do not.**

## Limit what must be trusted

Long-term storage should not require an archive provider to receive *plaintext*, meaning unencrypted user data, or require a software provider to keep control over access to that data.

Riverhog is self-hosted and places the plaintext/encryption boundary there. It can accept and encrypt content without using its host as another plaintext storage tier. Archive storage receives encrypted archive material rather than requiring plaintext custody.

*Ingress* is the path by which data enters Riverhog. Ingress keeps custody of source data until Riverhog has finalized the archive, so accepting data is not itself treated as successful preservation.

The secret material needed for recovery is kept separately from the archive. The same independence applies to software entrusted with durable user content or evidence: continued access does not depend on continued permission from its provider.

In short, **archive storage does not need plaintext or recovery secrets, and long-term access does not depend on continued permission from a software provider.**

## Preserve meaning, not only bytes

An artifact can survive intact while losing much of its usefulness if the context that gives it meaning disappears.

Knowing what a file contains may not tell us where it came from, what was observed about it, or how it passed between custodians.

Riverhog therefore preserves *provenance*: the recorded custody history of each file. That history is *append-only*, meaning new records can be added without rewriting earlier ones.

When data passes from one custodian to another, the history already recorded is preserved. If part of that history is left out, the omission requires an explicit reason.

Riverhog can build other representations to search or query provenance. Those representations can be replaced or rebuilt. They do not replace the recorded history itself.

In short, **recorded provenance stays with the artifact and can be extended, but not rewritten.**

## Keep the catalog rebuildable

People still need to find what they preserved without first recovering an entire archive.

Riverhog keeps a *catalog*: information used to find collections and artifacts. It can support discovery through collection and artifact identity, descriptions, classification tags, provenance, and other recorded information.

Some collection information can change without changing the collection’s archived contents. Descriptions and classification tags are examples. Riverhog keeps them with archive copies so they are not entrusted only to the operational database.

Search indexes and other catalog representations can be rebuilt from preserved information. Applications may provide richer ways to search, organize, or interpret that information without making their own state part of archive authority.

The catalog can therefore become more useful over time without becoming a requirement for recovery.

In short, **the catalog may help people find and search the archive, but it does not define the archive.**

## Let Riverhog become obsolete

A personal archive may outlive its storage provider, its applications, and Riverhog itself.

Recovery therefore starts from the archive, not from a working Riverhog deployment or database.

A complete archive copy, together with the separately kept key material it requires, contains what is needed to recover the files, provenance, and collection information preserved with that copy using independently available tools.

Riverhog may provide [recovery tools](../some-implementations/riverhog/recovery/) and other conveniences, but those tools are not what makes the archive recoverable.

Some collection information can change over time. An isolated archive copy can show only the version preserved with that copy. It cannot prove that no newer description or set of tags exists elsewhere. That does not change the identity of the archived collection.

In short, **a complete archive copy and its required key material are enough to recover what that copy preserves without Riverhog running.**

## Expect integrations to change

Long-lived data will encounter storage providers, applications, ingestion methods, processors, and interfaces that do not exist yet.

Riverhog therefore exposes *public contracts*: published interfaces that other software can rely on, rather than requiring integrations to become part of the archive core.

Applications own their workflows, state, and interfaces. Adapters translate between Riverhog and external systems. Components can perform specific kinds of work through Riverhog’s public contracts.

Each integration remains authoritative only for the things it owns. Taking part in a Riverhog workflow does not give it authority over unrelated archive state.

Components that are selected independently also keep separate identities, even when they share dependencies or deployment machinery.

In short, **integrations can be replaced without redefining the archive or gaining authority over unrelated state.**

## How this maps to the repository

The repository follows the same boundaries described above:

- [`riverhog`](../riverhog/): the code for Riverhog itself, including its archive and catalog responsibilities.
- [`packages`](../packages/): the public contracts and supporting code used across those boundaries.
- [`some-implementations`](../some-implementations/): supplied applications, adapters, components, and recovery tools built around those public contracts.

The repository therefore reflects the same architectural choice as the system itself: **keep archive authority clear and durable while keeping the systems around it replaceable.**

Storage providers, operational state, indexes, applications, adapters, components, and Riverhog itself can change without redefining the archive or separating it from the meaning needed to recover and understand it.
