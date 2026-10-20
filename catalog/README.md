# Shared catalog snapshot

This repository consumes the canonical HospedaPlus reference catalog maintained by
[`HospedaPlus/db_simulated`](https://github.com/HospedaPlus/db_simulated/blob/main/catalog/catalog.json).

- Pinned catalog version: `1.1.0`
- Local copy: `catalog/catalog.json`
- Consumers: F2 event generator and contract tests

Do not independently edit hotel codes, currencies, room types, channel codes, or
channel ownership in this repository. When the canonical catalog changes, review
the upstream change, copy the approved version, update the pinned version here,
and run the catalog contract tests.

The event generator uses canonical hotel codes and hotel currencies, canonical room
type codes and capacities, and only channels whose catalog `source` is `F2`.
Generated rates remain synthetic test values and are not the canonical pricing rules.
