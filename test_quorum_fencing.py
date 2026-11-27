"""Unit tests for the simulated quorum leader fencing validator."""

import unittest

from quorum_fencing import QuorumCluster, run_scenario


def base_cluster():
    return QuorumCluster(broker_ids=[0, 1, 2], leader=0,
                         isr=[0, 1, 2], leader_epoch=0)


class LeaderSwitchTest(unittest.TestCase):
    """Leader switch: old leader briefly alive, writes fenced by epoch."""

    def setUp(self):
        self.cluster = base_cluster()
        accepted = self.cluster.write(0, "p1", 0, 0, "a")
        self.assertEqual(accepted.status, "accepted")
        # Controller moves leadership to broker 1 with a shrunk ISR; the
        # old leader (broker 0) is NOT notified and stays briefly alive.
        self.cluster.elect_leader(leader=1, isr=[1, 2], leader_epoch=1)

    def test_old_leader_still_believes_it_leads(self):
        self.assertTrue(self.cluster.brokers[0].is_leader)
        self.assertEqual(self.cluster.brokers[0].leader_epoch, 0)
        self.assertEqual(self.cluster.controller_epoch, 1)

    def test_old_epoch_write_acked_but_not_quorum_accepted(self):
        result = self.cluster.write(0, "p1", 0, 1, "b")
        self.assertTrue(result.acked)
        self.assertFalse(result.quorum_accepted)
        self.assertEqual(result.status, "stale_leader_epoch")
        self.assertEqual(len(self.cluster.illegal_writes), 1)
        self.assertEqual(self.cluster.illegal_writes[0]["offset"], 1)

    def test_new_quorum_accepts_write_on_new_leader(self):
        result = self.cluster.write(1, "p1", 0, 1, "c")
        self.assertTrue(result.quorum_accepted)
        self.assertEqual(result.status, "accepted")
        self.assertEqual(result.offset, 1)

    def test_consumer_reads_new_quorum_order_only(self):
        self.cluster.write(0, "p1", 0, 1, "b")
        self.cluster.write(1, "p1", 0, 1, "c")
        view = self.cluster.read(consumer_offset=0)
        self.assertEqual(view["leader"], 1)
        self.assertEqual(view["leader_epoch"], 1)
        self.assertEqual(view["visible_offsets"], [0, 1])
        self.assertEqual(view["values"], ["a", "c"])
        self.assertNotIn("b", view["values"])

    def test_uncommitted_offset_not_visible(self):
        self.cluster.write(1, "p1", 0, 1, "c")
        view = self.cluster.read(consumer_offset=2)
        self.assertEqual(view["visible_offsets"], [])


class RecoveryBoundaryTest(unittest.TestCase):
    """Old leader rejoins: divergent suffix truncated at the boundary."""

    def test_sync_truncates_divergent_log(self):
        cluster = base_cluster()
        cluster.write(0, "p1", 0, 0, "a")
        cluster.elect_leader(leader=1, isr=[1, 2], leader_epoch=1)
        cluster.write(0, "p1", 0, 1, "b")
        cluster.write(1, "p1", 0, 1, "c")

        boundary = cluster.sync_broker(0)
        self.assertEqual(boundary, 1)
        self.assertEqual(cluster.recovery_boundary, 1)
        self.assertEqual(cluster.truncated_offsets, [1])

        old = cluster.brokers[0]
        self.assertFalse(old.is_leader)
        self.assertEqual(old.leader_epoch, 1)
        self.assertEqual([e.value for e in old.log], ["a"])

    def test_no_sync_means_stale_leader_keeps_serving(self):
        cluster = base_cluster()
        cluster.write(0, "p1", 0, 0, "a")
        cluster.elect_leader(leader=1, isr=[1, 2], leader_epoch=1)
        result = cluster.write(0, "p1", 0, 1, "b")
        self.assertEqual(result.status, "stale_leader_epoch")
        self.assertIsNone(cluster.recovery_boundary)


class IsrChangeTest(unittest.TestCase):
    """ISR shrink/expand moves the committed boundary but never backwards."""

    def test_isr_shrink_commits_with_remaining_members(self):
        cluster = base_cluster()
        cluster.write(0, "p1", 0, 0, "a")
        cluster.elect_leader(leader=1, isr=[1], leader_epoch=1)
        result = cluster.write(1, "p1", 0, 1, "b")
        self.assertTrue(result.quorum_accepted)
        leader = cluster.brokers[1]
        self.assertEqual(leader.high_watermark, 2)

    def test_isr_expand_with_lagging_member_does_not_regress_hw(self):
        cluster = base_cluster()
        cluster.write(0, "p1", 0, 0, "a")
        cluster.elect_leader(leader=1, isr=[1], leader_epoch=1)
        cluster.write(1, "p1", 0, 1, "b")
        leader = cluster.brokers[1]
        self.assertEqual(leader.high_watermark, 2)
        # Broker 2 lags at LEO 1; re-adding it must not move HW backwards.
        cluster.shrink_isr([1, 2])
        self.assertEqual(leader.high_watermark, 2)

    def test_isr_change_visible_in_scenario_report(self):
        report = run_scenario({
            "brokers": [0, 1, 2],
            "leader": 0,
            "isr": [0, 1, 2],
            "leader_epoch": 0,
            "events": [
                {"type": "write", "broker": 0, "producer_id": "p1",
                 "producer_epoch": 0, "sequence": 0, "value": "a"},
                {"type": "elect", "leader": 1, "isr": [1, 2],
                 "leader_epoch": 1},
                {"type": "isr_change", "isr": [1]},
                {"type": "write", "broker": 1, "producer_id": "p1",
                 "producer_epoch": 0, "sequence": 1, "value": "b"},
                {"type": "read", "consumer_offset": 0},
            ],
        })
        self.assertEqual(report["isr"], [1])
        self.assertEqual(report["writes"][-1]["status"], "accepted")
        self.assertEqual(report["reads"][0]["visible_offsets"], [0, 1])


class ProducerFencingTest(unittest.TestCase):
    """Producer epoch regression is fenced; newer epoch resets sequences."""

    def test_epoch_regression_fenced(self):
        cluster = base_cluster()
        cluster.write(0, "p1", 1, 0, "a")
        result = cluster.write(0, "p1", 0, 1, "b")
        self.assertFalse(result.acked)
        self.assertFalse(result.quorum_accepted)
        self.assertEqual(result.status, "producer_fenced_epoch_regression")
        self.assertEqual(len(cluster.brokers[0].log), 1)

    def test_newer_epoch_resets_sequence_space(self):
        cluster = base_cluster()
        cluster.write(0, "p1", 0, 0, "a")
        result = cluster.write(0, "p1", 1, 0, "b")
        self.assertEqual(result.status, "accepted")
        self.assertEqual(result.offset, 1)

    def test_zombie_producer_fenced_after_leader_switch(self):
        cluster = base_cluster()
        cluster.write(0, "p1", 1, 0, "a")
        cluster.elect_leader(leader=1, isr=[1, 2], leader_epoch=1)
        result = cluster.write(1, "p1", 0, 1, "b")
        self.assertEqual(result.status, "producer_fenced_epoch_regression")


class DuplicateAckTest(unittest.TestCase):
    """Duplicate sequences get idempotent ACKs without re-appending."""

    def test_duplicate_ack_does_not_reappend(self):
        cluster = base_cluster()
        first = cluster.write(0, "p1", 0, 0, "a")
        self.assertEqual(first.status, "accepted")
        dup = cluster.write(0, "p1", 0, 0, "a")
        self.assertTrue(dup.acked)
        self.assertTrue(dup.quorum_accepted)
        self.assertEqual(dup.status, "duplicate_ack")
        self.assertEqual(len(cluster.brokers[0].log), 1)

    def test_sequence_gap_rejected(self):
        cluster = base_cluster()
        cluster.write(0, "p1", 0, 0, "a")
        result = cluster.write(0, "p1", 0, 2, "c")
        self.assertFalse(result.acked)
        self.assertEqual(result.status, "out_of_order_sequence")


class ScenarioReportTest(unittest.TestCase):
    """End-to-end scenario: illegal writes, visible offsets, boundary."""

    def test_full_report(self):
        report = run_scenario({
            "brokers": [0, 1, 2],
            "leader": 0,
            "isr": [0, 1, 2],
            "leader_epoch": 0,
            "events": [
                {"type": "write", "broker": 0, "producer_id": "p1",
                 "producer_epoch": 0, "sequence": 0, "value": "a"},
                {"type": "elect", "leader": 1, "isr": [1, 2],
                 "leader_epoch": 1},
                {"type": "write", "broker": 0, "producer_id": "p1",
                 "producer_epoch": 0, "sequence": 1, "value": "stale"},
                {"type": "write", "broker": 1, "producer_id": "p1",
                 "producer_epoch": 0, "sequence": 1, "value": "b"},
                {"type": "write", "broker": 1, "producer_id": "p1",
                 "producer_epoch": 0, "sequence": 1, "value": "b"},
                {"type": "sync", "broker": 0},
                {"type": "read", "consumer_offset": 0},
            ],
        })
        statuses = [w["status"] for w in report["writes"]]
        self.assertEqual(statuses, ["accepted", "stale_leader_epoch",
                                    "accepted", "duplicate_ack"])
        self.assertEqual(len(report["illegal_writes"]), 1)
        self.assertEqual(report["illegal_writes"][0]["broker"], 0)
        self.assertEqual(report["recovery_boundary"], 1)
        self.assertEqual(report["truncated_offsets"], [1])
        read = report["reads"][0]
        self.assertEqual(read["visible_offsets"], [0, 1])
        self.assertEqual(read["values"], ["a", "b"])

    def test_non_leader_write_rejected(self):
        report = run_scenario({
            "brokers": [0, 1],
            "leader": 0,
            "isr": [0, 1],
            "events": [
                {"type": "write", "broker": 1, "producer_id": "p1",
                 "producer_epoch": 0, "sequence": 0, "value": "x"},
            ],
        })
        self.assertEqual(report["writes"][0]["status"],
                         "not_leader_for_partition")
        self.assertFalse(report["writes"][0]["acked"])


if __name__ == "__main__":
    unittest.main()
