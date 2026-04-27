@acceptance @api
Feature: Read-only hot storage browsing
  Completed hot files become browseable only after Riverhog promotes verified
  bytes into the committed hot namespace.

  Rule: Read-only browsing exposes committed hot files only
    # Acceptance backing for the WebDAV browsing surface is tracked by Issue #89.

    @xfail_not_backed
    Scenario: The read-only browsing surface hides staging paths
      Given an archive containing collection "docs"
      When the client lists the read-only browsing root
      Then the read-only browsing surface exposes path "docs/tax/2022/invoice-123.pdf"
      And the read-only browsing surface hides path ".arc/"

    @xfail_not_backed
    Scenario: The read-only browsing surface rejects writes
      When the client attempts to write "forbidden.txt" through the read-only browsing surface
      Then the read-only browsing write is rejected
