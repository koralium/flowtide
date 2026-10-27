---
sidebar_position: 3
---

# Requirements for Fault-Tolerant Data Consistency

This document outlines how to guarantee data consistency in a data stream, how the flow works, and what rules must be met. 

These guarantees are not required for every use case. For example, low latency might be a higher priority than strong consistency, such as when streaming updates to a search engine like Elasticsearch, where eventual consistency is acceptable. However, this document defines what is required when strict consistency across crashes is necessary.

## Required functions

* **OnInitialize** - called when a stream is started, from a crash or a restart.
* **OnCheckpointComplete** - Called when a checkpoint has been completed and state data is persisted on a storage device.
* **UploadDelta** - A function that uploads delta changes to a staging destination.
* **CommitProcedure** ($P$) — Merges staged delta records into the final destination store.

## System Model & Core Contract

**State Variables:**

* $v_{\text{cp}} \in \mathbb{N}$ : Last durable checkpoint version committed to disk.
* $\mathcal{D}$ : Staging delta relation holding records $(k, \text{op}, d, v)$ where $v \in \mathbb{N}$. $k$ is the key (row identity), $d$ is the data.
* $\mathcal{S}$ : Authoritative destination relation representing the sum of all committed deltas:

$$\mathcal{S} = \sum_{i=1}^{v_{\text{cp}}} \Delta_i = \Delta_1 \oplus \Delta_2 \oplus \dots \oplus \Delta_{v_{\text{cp}}}$$

### When is Idempotency Required?


In this architecture, idempotency in the commit procedure is necessary when all of these conditions are met:

1. **Mutate-in-Place Destination:** Unlike immutable systems (such as Delta Lake or append-only logs that advance an atomic metadata pointer), this sink updates existing records directly in the target store.
2. **Decoupled Commit Barriers:** The engine's checkpoint state and the database commit are distinct operations without two-phase commit (2PC).
3. **Replay Ambiguity:** If a failure occurs immediately after a checkpoint is persisted to disk, and the destination system cannot determine whether P completed successfully or was interrupted, OnInitialize must re-invoke $P$ safely without introducing duplicate rows or corrupting aggregated state.


**Contract: Idempotence Requirement**

If the destination has issues with the points above, the commit procedure $P$ must be strictly idempotent across identical version boundaries:

$$\forall \mathcal{D}, \mathcal{S}, v: \quad P(\mathcal{D}, P(\mathcal{D}, \mathcal{S}, v), v) = P(\mathcal{D}, \mathcal{S}, v)$$

Executing $P$ multiple times with the same latest version parameter $v$ over staging data $\mathcal{D}$ must yield the exact same destination state $\mathcal{S}$ without side effects, phantom records, or duplicate modifications.

The procedure does not need to be idempotent across historical versions, only for re-executions against the current $v_{\text{cp}}$.

## Algorithm

```
procedure OnInitialize()
    v ← ReadCheckpointFromDisk()
    
    // 1. Rollback uncommitted speculative writes
    D ← { r ∈ D | r.version ≤ v }
    
    // 2. Resynchronize destination state
    S ← CommitProcedure(D, S, v)
end procedure


procedure UploadDelta(batch)
    v_next ← v_cp + 1
    
    // Upload uncommitted changes to staging
    for each (key, op, data) in batch do
        D ← D ∪ { (key, op, data, v_next) }
    end for
end procedure


procedure OnCheckpointComplete(v_new)
    // 1. Atomic write barrier to disk
    SaveCheckpointToDisk(v_new)
    v_cp ← v_new
    
    // 2. Materialize confirmed deltas into destination
    S ← CommitProcedure(D, S, v_cp)
end procedure


function CommitProcedure(D, S, v)
    // Ingest confirmed staging deltas
    Δ_valid ← { r ∈ D | r.version ≤ v }
    
    // Deterministic state fold
    S_next ← S ⊕ Δ_valid
    return S_next
end function
```

## Correctness Analysis & Zero Data Loss Guarantee

The destination state represents the sum of all confirmed changes up to the durable checkpoint:

$$\mathcal{S}^* = \sum_{i=1}^{v_{\text{cp}}} \Delta_i$$

Data loss occurs if any committed delta $\Delta_i$ ($i \le v_{\text{cp}}$) is missing from $\mathcal{S}^*$. Duplicate corruption occurs if applying a delta multiple times shifts the state beyond $\mathcal{S}^*$.

**1. Crash During Inflight Upload ($r.v > v_{\text{cp}}$)**

* **Failure State:** Incomplete delta fragments exist in staging ($\exists r \in \mathcal{D} : r.v = v_{\text{cp}} + 1$). Destination $\mathcal{S}$ is unaffected at $\mathcal{S}(v_{\text{cp}})$.
* **Recovery:** OnInitialize purges incomplete rows:$$\mathcal{D}_0 = \{ r \in \mathcal{D} \mid r.v \le v_{\text{cp}} \}$$
* **Outcome:** Uncommitted writes are cleanly removed. The stream replays the batch starting from checkpoint $v_{\text{cp}}$, regenerating $\Delta_{v_{\text{cp}} + 1}$ deterministically. No uncommitted data enters $\mathcal{S}$, and no committed data is dropped.


**2. Crash During or After Checkpoint ($r.v \le v_{\text{cp}}$)**

* **Failure State:** $v_{\text{cp}}$ is updated on disk, but the node crashes during or immediately following CommitProcedure.

* **Recovery:** On restart, OnInitialize reads durable $v_{\text{cp}}$ and re-invokes the procedure:$$\mathcal{S} \leftarrow P(\mathcal{D}_0, \mathcal{S}, v_{\text{cp}})$$

* **Outcome:** Because $P$ satisfies the idempotency contract:$$P(\mathcal{D}, P(\mathcal{D}, \mathcal{S}, v_{\text{cp}}), v_{\text{cp}}) = P(\mathcal{D}, \mathcal{S}, v_{\text{cp}})$$Re-running the procedure produces the exact same destination state. No records are double-counted or duplicated.

### Core Guarantees

Following any recovery or completed checkpoint, the protocol deterministically enforces:

$$\mathcal{S} \equiv \sum_{i=1}^{v_{\text{cp}}} \Delta_i \quad \wedge \quad \forall r \in \mathcal{D}, \; r.v \le v_{\text{cp}}$$


## Implementation examples

In this section, different destinations are given as examples and how they can fulfill this protocol.

### Delta Lake

In delta lake the staging area for new updates are new parquet files and a temporary delta log file that is not yet renamed to a version number.

The commit procedure checks if the latest version is referenced in the delta log if so it can stop. Otherwise it checks if any uncommited delta log file exist for the current version. If so, it renames the file to a real commit log version file to commit the data.

A rollback can be handled in two ways, either delete any data files that exist in the folder structure that are not referenced delta log, or ignore those files since they are not referenced and will not affect the data outcome.


### SQL Server without transactions

In this SQL Server example, transactions are not used to reduce locking on the destination table.

In SQL Server, the destination is a mutable relational table, and the staging area is a dedicated staging table holding the delta rows along with their stream version.

The commit procedure can do a merge from the staging area into the final destination table.
If the destination table uses primary keys, this becomes idempotent, upserting a row with an identical key will not change the outcome of the row. Same when deleting a key, if it is already deleted, the merge delete will be a no-op.


A rollback is handled in OnInitialize by running:

```
DELETE FROM StagingTable WHERE version > @v_cp
```

This purges any partial or speculative rows left behind by a crash before the stream resumes.

### SQL Server with Two-Phase Commit

With 2PC (via MSDTC), no staging table or idempotent merge is needed. The database transaction log becomes the staging area, and changes are written directly to the destination table inside a distributed transaction.

In UploadDelta, rows are written directly to the destination within the open transaction.

During checkpoint, the engine asks SQL Server to prepare the transaction. Once the engine has safely saved its checkpoint to disk, OnCheckpointComplete commits the transaction.

In OnInitialize, the engine checks for any hanging prepared transactions:
* If a transaction matches a version $\le v_{\text{cp}}$, it commits it.
* If it is for a newer version $> v_{\text{cp}}$, or was never prepared, it rolls it back.

This avoids staging tables and merge logic, but keeps locks on the destination table during the checkpoint.