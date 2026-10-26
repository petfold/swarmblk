# Engine read path and prefetch — 26 October 2026

First boots through the engine (`swarmblk serve`, ROADMAP Phase 2): the CirrOS guest of the [Phase 1 gate](phase1-gate-2026-10-26.md) booted from a qcow2 overlay whose backing file is the engine's NBD export of the gate's lineage. The guest prints the marker written at the gate and powers off. Each run starts the engine with an empty local store unless noted.

## Runs

| Run | Bee node | Engine store | Prefetch | Boot to power-off | Chunks fetched from Bee |
|---|---|---|---|---|---|
| HTTP backing (Phase 1 gate, for comparison) | A | — | QEMU and Bee readahead | 20 s | — |
| engine, cold | A, fetching back from the network | empty | none | 103 s | 6,666 |
| engine, warm | A | holds every chunk | none | 14.3 s | 6 |
| engine, recording the trace | A | holds every chunk | none | 14.1 s | 0 |
| engine through node B | B, chunks in its cache | empty | none | 22.7 s | 6,647 |
| engine through node B | B, chunks in its cache | empty | the recorded trace | **15.4 s** | 6,647 |

The trace holds 7,077 blocks (27.6 MiB) in 290 runs. Runs through node B used an SSH tunnel and `serve -nocache`.

## Observations

1. **Without prefetch the guest waits for one round trip per read.** The engine fetches only what the guest asks for, and a booting guest asks for little at a time. Node A had dropped its uploaded chunks after syncing them (they were not pinned), so the cold run fetched them back from the network one guest read at a time: 103 s. The HTTP backing hid this with QEMU's and Bee's readahead.
2. **Prefetch hides it.** Through node B, which had these chunks cached from the gate's export, the same boot took 22.7 s without prefetch and 15.4 s with the trace, about the 14 s of a boot from the local store: the guest found its blocks fetched or in flight.
3. **These runs are not cold on Swarm.** Both nodes had fetched this disk before. A cold measurement needs a disk that neither node has seen, read with `-nocache` so that runs stay cold: next, with a larger image, to set the cold-boot bound that Phase 0 left for an engine prototype.
