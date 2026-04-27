@acceptance @api
Feature: Read-only hot storage browsing
  Completed hot files become browseable only after Riverhog promotes verified
  bytes into the committed hot namespace.

  Rule: Read-only browsing exposes committed hot files only
    Scenario: The read-only browsing surface hides staging paths
      Given an archive containing collection "docs"
      When the client lists the read-only browsing root
      Then the read-only browsing surface exposes path "docs/tax/2022/invoice-123.pdf"
      And the read-only browsing surface hides path ".arc/"

    Scenario: The read-only browsing surface rejects writes
      When the client attempts to write "forbidden.txt" through the read-only browsing surface
      Then the read-only browsing write is rejected

    Scenario: The canonical storage bucket publishes incomplete multipart cleanup
      When the client inspects the canonical storage lifecycle configuration
      Then the storage lifecycle aborts incomplete multipart uploads after 3 days
