"""Simulated quorum leader fencing validator.

All broker, controller, producer and consumer states in this module are
SIMULATED in-process. No real network, disk or ZooKeeper/KRaft traffic is
involved; the goal is to verify fencing semantics after a leader switch:

* a stale leader that briefly stays alive must not get its writes accepted
  by the new quorum (leader-epoch fencing),
* a producer whose epoch regresses must be fenced (producer-epoch fencing),
* duplicate sequences must produce duplicate ACKs without re-appending,
* ISR shrink/expand changes which offsets are committed (high watermark),
* a rejoining old leader truncates its divergent log at the recovery
  boundary (the new leader's high watermark).

The scenario is driven by a list of events (see ``run_scenario``) and the
result is a JSON-serialisable report containing, per write, whether the new
quorum accepted it, the list of illegal writes, the offsets visible to a
consumer and the recovery boundary.

Compatible with Python 3.8.2.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set


@dataclass
class LogEntry:
    offset: int
    producer_id: str
    producer_epoch: int
    sequence: int
    value: Any


@dataclass
class Broker:
    """A simulated broker. All fields are local simulation state only."""

    broker_id: int
    leader_epoch: int = 0
    is_leader: bool = False
    isr: Set[int] = field(default_factory=set)
    high_watermark: int = 0
    log: List[LogEntry] = field(default_factory=list)
    producer_state: Dict[str, List[int]] = field(default_factory=dict)

    @property
    def log_end_offset(self) -> int:
        return self.log[-1].offset + 1 if self.log else 0


@dataclass
class WriteResult:
    broker: int
    producer_id: str
    producer_epoch: int
    sequence: int
    acked: bool
    quorum_accepted: bool
    status: str
    offset: Optional[int] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "broker": self.broker,
            "producer_id": self.producer_id,
            "producer_epoch": self.producer_epoch,
            "sequence": self.sequence,
            "acked": self.acked,
            "quorum_accepted": self.quorum_accepted,
            "status": self.status,
            "offset": self.offset,
        }


class QuorumCluster:
    """Simulated controller + brokers used to validate fencing behaviour."""

    def __init__(self, broker_ids, leader, isr, leader_epoch=0):
        self.controller_epoch = leader_epoch
        self.leader = leader
        self.isr: Set[int] = set(isr)
        self.brokers: Dict[int, Broker] = {}
        for bid in broker_ids:
            self.brokers[bid] = Broker(
                broker_id=bid,
                leader_epoch=leader_epoch,
                is_leader=(bid == leader),
                isr=set(isr),
            )
        self.write_results: List[WriteResult] = []
        self.illegal_writes: List[Dict[str, Any]] = []
        self.recovery_boundary: Optional[int] = None
        self.truncated_offsets: List[int] = []

    def elect_leader(self, leader, isr, leader_epoch):
        """Controller elects a new leader; old leader may stay briefly alive."""
        if leader_epoch <= self.controller_epoch:
            raise ValueError("leader epoch must increase on election")
        self.controller_epoch = leader_epoch
        self.leader = leader
        self.isr = set(isr)
        new_leader = self.brokers[leader]
        new_leader.is_leader = True
        new_leader.leader_epoch = leader_epoch
        new_leader.isr = set(isr)
        new_leader.high_watermark = min(
            (self.brokers[b].log_end_offset for b in self.isr
             if b in self.brokers),
            default=new_leader.log_end_offset,
        )
        # Followers in the new ISR learn the epoch; the OLD leader is
        # deliberately NOT notified here so the "briefly alive" window can
        # be exercised. It learns the new epoch only via ``sync_broker``.
        stale = self._stale_leader_id()
        for bid in self.isr:
            if bid != leader and bid != stale:
                follower = self.brokers[bid]
                follower.is_leader = False
                follower.leader_epoch = leader_epoch
                follower.isr = set(isr)

    def shrink_isr(self, isr):
        """Controller shrinks/expands the ISR on the current leader."""
        self.isr = set(isr)
        leader = self.brokers[self.leader]
        leader.isr = set(isr)
        # The high watermark is monotonic: an ISR change can advance it
        # (shrink) or hold it (waiting for a lagging re-added member), but
        # never move it backwards.
        committed = min(
            (self.brokers[b].log_end_offset for b in self.isr
             if b in self.brokers),
            default=leader.log_end_offset,
        )
        leader.high_watermark = max(leader.high_watermark, committed)

    def sync_broker(self, broker_id):
        """A stale broker learns the current epoch and rejoins as follower.

        Its divergent suffix beyond the leader's high watermark is
        truncated; that watermark is the recovery boundary.
        """
        broker = self.brokers[broker_id]
        leader = self.brokers[self.leader]
        broker.is_leader = False
        broker.leader_epoch = self.controller_epoch
        broker.isr = set(self.isr)
        # Anything the stale broker accepted locally beyond its own high
        # watermark was never committed by the quorum and is divergent.
        boundary = min(broker.high_watermark, leader.high_watermark)
        divergent = [e.offset for e in broker.log if e.offset >= boundary]
        broker.log = [e for e in broker.log if e.offset < boundary]
        broker.high_watermark = min(broker.high_watermark, boundary)
        self.recovery_boundary = boundary
        self.truncated_offsets = divergent
        return boundary

    def write(self, broker_id, producer_id, producer_epoch, sequence, value):
        """Attempt to append a record through the given broker."""
        broker = self.brokers[broker_id]
        result = self._append(broker, producer_id, producer_epoch,
                              sequence, value)
        self.write_results.append(result)
        if result.acked and not result.quorum_accepted:
            self.illegal_writes.append(result.as_dict())
        return result

    def _append(self, broker, producer_id, producer_epoch, sequence, value):
        is_current_leader = (
            broker.is_leader
            and broker.broker_id == self.leader
            and broker.leader_epoch == self.controller_epoch
        )

        # Leader-epoch fencing: the broker still thinks it leads with an
        # old epoch. It ACKs locally (briefly alive) but the new quorum
        # never sees the record.
        if broker.is_leader and broker.leader_epoch < self.controller_epoch:
            offset = self._local_append(broker, producer_id, producer_epoch,
                                        sequence, value)
            return WriteResult(broker.broker_id, producer_id, producer_epoch,
                               sequence, acked=True, quorum_accepted=False,
                               status="stale_leader_epoch", offset=offset)

        if not is_current_leader:
            return WriteResult(broker.broker_id, producer_id, producer_epoch,
                               sequence, acked=False, quorum_accepted=False,
                               status="not_leader_for_partition")

        # Producer-epoch fencing on the current leader.
        state = broker.producer_state.get(producer_id)
        if state is not None and producer_epoch < state[0]:
            return WriteResult(broker.broker_id, producer_id, producer_epoch,
                               sequence, acked=False, quorum_accepted=False,
                               status="producer_fenced_epoch_regression")
        # A newer producer incarnation resets the sequence space.
        if state is not None and producer_epoch > state[0]:
            state[0] = producer_epoch
            state[1] = 0

        # Sequence handling: duplicates are ACKed idempotently, gaps fail.
        if state is not None and producer_epoch == state[0]:
            expected = state[1]
            if sequence < expected:
                return WriteResult(broker.broker_id, producer_id,
                                   producer_epoch, sequence, acked=True,
                                   quorum_accepted=True,
                                   status="duplicate_ack", offset=None)
            if sequence > expected:
                return WriteResult(broker.broker_id, producer_id,
                                   producer_epoch, sequence, acked=False,
                                   quorum_accepted=False,
                                   status="out_of_order_sequence")

        offset = self._local_append(broker, producer_id, producer_epoch,
                                    sequence, value)
        entry = broker.log[-1]
        # Eager replication to in-sync followers that share the current
        # controller epoch (a stale broker is excluded by definition).
        for bid in self.isr:
            follower = self.brokers.get(bid)
            if (follower is None or follower is broker
                    or follower.leader_epoch != self.controller_epoch):
                continue
            follower.log.append(LogEntry(entry.offset, entry.producer_id,
                                         entry.producer_epoch,
                                         entry.sequence, entry.value))
            fstate = follower.producer_state.setdefault(
                entry.producer_id, [entry.producer_epoch, 0])
            fstate[0] = max(fstate[0], entry.producer_epoch)
            if entry.sequence >= fstate[1]:
                fstate[1] = entry.sequence + 1
        # Advance the high watermark to what every ISR member holds. In
        # this simulation followers replicate eagerly, so the committed
        # boundary is the LEO of the slowest ISR member.
        broker.high_watermark = min(
            (self.brokers[b].log_end_offset for b in self.isr
             if b in self.brokers),
            default=broker.log_end_offset,
        )
        return WriteResult(broker.broker_id, producer_id, producer_epoch,
                           sequence, acked=True, quorum_accepted=True,
                           status="accepted", offset=offset)

    @staticmethod
    def _local_append(broker, producer_id, producer_epoch, sequence, value):
        offset = broker.log_end_offset
        broker.log.append(LogEntry(offset, producer_id, producer_epoch,
                                   sequence, value))
        state = broker.producer_state.setdefault(producer_id,
                                                 [producer_epoch, 0])
        state[0] = max(state[0], producer_epoch)
        if sequence >= state[1]:
            state[1] = sequence + 1
        return offset

    def read(self, consumer_offset):
        """Consumer read from the CURRENT leader at ``consumer_offset``.

        Only offsets below the leader's high watermark are visible, and the
        ordering is whatever the new quorum's log dictates.
        """
        leader = self.brokers[self.leader]
        visible = [e for e in leader.log
                   if e.offset >= consumer_offset
                   and e.offset < leader.high_watermark]
        return {
            "leader": leader.broker_id,
            "leader_epoch": leader.leader_epoch,
            "consumer_offset": consumer_offset,
            "high_watermark": leader.high_watermark,
            "visible_offsets": [e.offset for e in visible],
            "values": [e.value for e in visible],
        }

    def report(self):
        return {
            "controller_epoch": self.controller_epoch,
            "leader": self.leader,
            "isr": sorted(self.isr),
            "writes": [r.as_dict() for r in self.write_results],
            "illegal_writes": self.illegal_writes,
            "recovery_boundary": self.recovery_boundary,
            "truncated_offsets": self.truncated_offsets,
        }

    def _stale_leader_id(self):
        for broker in self.brokers.values():
            if broker.is_leader and broker.broker_id != self.leader:
                return broker.broker_id
        return None


def run_scenario(scenario):
    """Run a scenario dict and return the JSON-serialisable report.

    Scenario format::

        {
          "brokers": [0, 1, 2],
          "leader": 0, "isr": [0, 1, 2], "leader_epoch": 0,
          "events": [
            {"type": "write", "broker": 0, "producer_id": "p1",
             "producer_epoch": 0, "sequence": 0, "value": "a"},
            {"type": "elect", "leader": 1, "isr": [1, 2], "leader_epoch": 1},
            {"type": "isr_change", "isr": [1]},
            {"type": "sync", "broker": 0},
            {"type": "read", "consumer_offset": 0}
          ]
        }
    """
    cluster = QuorumCluster(
        broker_ids=scenario["brokers"],
        leader=scenario["leader"],
        isr=scenario["isr"],
        leader_epoch=scenario.get("leader_epoch", 0),
    )
    reads = []
    for event in scenario.get("events", []):
        etype = event["type"]
        if etype == "write":
            cluster.write(event["broker"], event["producer_id"],
                          event["producer_epoch"], event["sequence"],
                          event.get("value"))
        elif etype == "elect":
            cluster.elect_leader(event["leader"], event["isr"],
                                 event["leader_epoch"])
        elif etype == "isr_change":
            cluster.shrink_isr(event["isr"])
        elif etype == "sync":
            cluster.sync_broker(event["broker"])
        elif etype == "read":
            reads.append(cluster.read(event.get("consumer_offset", 0)))
        else:
            raise ValueError("unknown event type: %r" % etype)
    report = cluster.report()
    report["reads"] = reads
    return report


def main(argv=None):
    import json
    import sys

    argv = list(sys.argv[1:] if argv is None else argv)
    if argv:
        with open(argv[0], "r", encoding="utf-8") as fh:
            scenario = json.load(fh)
    else:
        scenario = json.load(sys.stdin)
    json.dump(run_scenario(scenario), sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
