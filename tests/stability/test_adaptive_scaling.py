import time
import uuid

import dask.array as da
import pytest
from coiled.v2 import Cluster
from dask import delayed
from dask.distributed import Client, Event, wait

TIMEOUT_THRESHOLD = 600  # 10 minutes


@pytest.mark.stability
@pytest.mark.parametrize("minimum,threshold", [(0, 300), (1, 150)])
def test_scale_up_on_task_load(minimum, threshold):
    """Tests that adaptive scaling reacts in a reasonable amount of time to
    an increased task load and scales up.
    """
    maximum = 10
    with Cluster(
        name=f"test_adaptive_scaling-{uuid.uuid4().hex}",
        n_workers=minimum,
        worker_vm_types=["t3.medium"],
        wait_for_workers=True,
        # Note: We set allowed-failures to ensure that no tasks are not retried upon ungraceful shutdown behavior
        # during adaptive scaling but we receive a KilledWorker() instead.
        environ={"DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES": "0"},
    ) as cluster:
        with Client(cluster) as client:
            adapt = cluster.adapt(minimum=minimum, maximum=maximum)
            time.sleep(adapt.interval * 2.1)  # Ensure enough time for system to adapt
            assert len(adapt.log) == 0
            ev_fan_out = Event(name="fan-out", client=client)

            def clog(x: int, ev: Event) -> int:
                ev.wait()
                return x

            futures = client.map(clog, range(100), ev=ev_fan_out)

            start = time.monotonic()
            client.wait_for_workers(n_workers=maximum, timeout=TIMEOUT_THRESHOLD)
            end = time.monotonic()
            duration = end - start
            assert duration < threshold, duration
            assert len(adapt.log) <= 2
            assert adapt.log[-1][1] == {"status": "up", "n": maximum}
            ev_fan_out.set()
            client.gather(futures)
            return duration


@pytest.mark.stability
@pytest.mark.stability
def test_adapt_to_changing_workload():
    """Tests that adaptive scaling reacts within a reasonable amount of time to
    a varying task load and scales up or down. This also asserts that no recomputation
    is caused by the scaling.
    """
    minimum = 0
    maximum = 10
    fan_out_size = 100
    with Cluster(
        name=f"test_adaptive_scaling-{uuid.uuid4().hex}",
        n_workers=maximum,
        worker_vm_types=["t3.medium"],
        wait_for_workers=True,
        # Note: We set allowed-failures to ensure that no tasks are not retried upon ungraceful shutdown behavior
        # during adaptive scaling but we receive a KilledWorker() instead.
        environ={"DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES": "0"},
    ) as cluster:
        with Client(cluster) as client:

            @delayed
            def clog(x: int, ev: Event, **kwargs) -> int:
                ev.wait()
                return x

            def workload(
                fan_out_size,
                ev_fan_out,
                ev_barrier,
                ev_final_fan_out,
            ):
                fan_out = [clog(i, ev=ev_fan_out) for i in range(fan_out_size)]
                barrier = clog(delayed(sum)(fan_out), ev=ev_barrier)
                final_fan_out = [
                    clog(i, ev=ev_final_fan_out, barrier=barrier)
                    for i in range(fan_out_size)
                ]
                return final_fan_out

            ev_fan_out = Event(name="fan-out", client=client)
            ev_barrier = Event(name="barrier", client=client)
            ev_final_fan_out = Event(name="final-fan-out", client=client)

            fut = client.compute(
                workload(
                    fan_out_size=fan_out_size,
                    ev_fan_out=ev_fan_out,
                    ev_barrier=ev_barrier,
                    ev_final_fan_out=ev_final_fan_out,
                )
            )

            adapt = cluster.adapt(minimum=minimum, maximum=maximum)
            wait(90)

            ev_fan_out.set()
            # Scale down to a single worker
            start = time.monotonic()
            while len(cluster.observed) > 1:
                if time.monotonic() - start >= TIMEOUT_THRESHOLD:
                    raise TimeoutError()
                time.sleep(0.1)
            end = time.monotonic()
            duration_first_scale_down = end - start
            assert duration_first_scale_down < 330
            assert len(cluster.observed) == 1
            assert adapt.log[-1][1]["status"] == "down"

            ev_barrier.set()
            # Scale up to maximum again
            start = time.monotonic()
            client.wait_for_workers(n_workers=maximum, timeout=TIMEOUT_THRESHOLD)
            end = time.monotonic()
            duration_second_scale_up = end - start
            assert duration_second_scale_up < 150
            assert len(cluster.observed) == maximum
            assert adapt.log[-1][1]["status"] == "up"

            ev_final_fan_out.set()
            client.gather(fut)
            del fut

            # Scale down to minimum
            start = time.monotonic()
            while len(cluster.observed) > minimum:
                if time.monotonic() - start >= TIMEOUT_THRESHOLD:
                    raise TimeoutError()
                time.sleep(0.1)
            end = time.monotonic()
            duration_second_scale_down = end - start
            assert duration_second_scale_down < 330
            assert len(cluster.observed) == minimum
            assert adapt.log[-1][1]["status"] == "down"
            return (
                duration_first_scale_down,
                duration_second_scale_up,
                duration_second_scale_down,
            )


@pytest.mark.stability
def test_adaptive_rechunk_stress():
    """Tests adaptive scaling in a transfer-heavy workload that reduces its memory load
    in a series of rechunking and dimensional reduction steps.
    """
    with Cluster(
        name=f"test_adaptive_scaling-{uuid.uuid4().hex}",
        n_workers=32,
        worker_vm_types=["t3.large"],
        wait_for_workers=True,
        # Note: We set allowed-failures to ensure that no tasks are not retried upon ungraceful shutdown behavior
        # during adaptive scaling but we receive a KilledWorker() instead.
        environ={"DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES": "0"},
    ) as cluster:
        with Client(cluster) as client:

            def workload(arr):
                arr = arr.sum(axis=[3])
                arr = (
                    arr.rechunk((128, 8 * 1024, 2))
                    .rechunk((8 * 1024, 128, 2))
                    .rechunk((128, 8 * 1024, 2))
                    .sum(axis=[2])
                )
                arr = (
                    arr.rechunk((64, 8 * 1024))
                    .rechunk((8 * 1024, 64))
                    .rechunk((64, 8 * 1024))
                    .sum(axis=[1])
                )
                return arr.sum()

            # Initialize array on workers to avoid adaptive scale-down
            arr = client.persist(
                da.random.random(
                    (8 * 1024, 8 * 1024, 16, 16), chunks=(8 * 1024, 128, 2, 2)
                )
            )
            wait(arr)

            cluster.adapt(
                minimum=1,
                maximum=32,
                interval="1s",
                target_duration="180s",
                wait_count=1,
            )
            fut = client.compute(workload(arr))
            del arr
            wait(fut)
            assert fut.result()


@pytest.mark.skip(
    reason="The test behavior is unreliable and may lead to very long runtime (see: coiled-runtime#211)"
)
@pytest.mark.stability
@pytest.mark.parametrize("minimum", (0, 1))
def test_adapt_to_memory_intensive_workload(minimum):
    """Tests that adaptive scaling reacts within a reasonable amount of time to a varying task and memory load.

    Note: This tests currently results in spilling and very long runtimes.
    """
    maximum = 10
    with Cluster(
        name=f"test_adaptive_scaling-{uuid.uuid4().hex}",
        n_workers=maximum,
        worker_vm_types=["t3.medium"],
        wait_for_workers=True,
        # Note: We set allowed-failures to ensure that no tasks are not retried upon ungraceful shutdown behavior
        # during adaptive scaling but we receive a KilledWorker() instead.
        environ={"DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES": "0"},
    ) as cluster:
        with Client(cluster) as client:

            def memory_intensive_processing():
                matrix = da.random.random((40000, 40000), chunks=(40000, 500))
                rechunked = matrix.rechunk((500, 40000))
                reduction = rechunked.sum()
                return reduction

            @delayed
            def clog(x, ev_start: Event, ev_barrier: Event):
                ev_start.set()
                ev_barrier.wait()
                return x

            def compute_intensive_barrier_task(
                data, ev_start: Event, ev_barrier: Event
            ):
                barrier = clog(data, ev_start, ev_barrier)
                return barrier

            ev_scale_down = Event(name="scale_down", client=client)
            ev_barrier = Event(name="barrier", client=client)

            fut = client.compute(
                compute_intensive_barrier_task(
                    memory_intensive_processing(), ev_scale_down, ev_barrier
                )
            )

            adapt = cluster.adapt(minimum=minimum, maximum=maximum)

            ev_scale_down.wait()
            # Scale down to a single worker on barrier task
            start = time.monotonic()
            while len(cluster.observed) > 1:
                if time.monotonic() - start >= TIMEOUT_THRESHOLD:
                    raise TimeoutError()
                time.sleep(0.1)
            end = time.monotonic()
            duration_first_scale_down = end - start
            assert duration_first_scale_down < 420, duration_first_scale_down
            assert len(cluster.observed) == 1
            assert adapt.log[-1][1]["status"] == "down"

            ev_barrier.set()
            wait(fut)
            fut = client.compute(memory_intensive_processing())

            # Scale up to maximum on postprocessing
            start = time.monotonic()
            client.wait_for_workers(n_workers=maximum, timeout=TIMEOUT_THRESHOLD)
            end = time.monotonic()
            duration_second_scale_up = end - start
            assert duration_second_scale_up < 420, duration_second_scale_up
            assert len(cluster.observed) == maximum
            assert adapt.log[-1][1]["status"] == "up"

            wait(fut)
            del fut

            # Scale down to minimum
            start = time.monotonic()
            while len(cluster.observed) > minimum:
                if time.monotonic() - start >= TIMEOUT_THRESHOLD:
                    raise TimeoutError()
                time.sleep(0.1)
            end = time.monotonic()
            duration_second_scale_down = end - start
            assert duration_second_scale_down < 420, duration_second_scale_down
            assert len(cluster.observed) == minimum
            assert adapt.log[-1][1]["status"] == "down"
            return (
                duration_first_scale_down,
                duration_second_scale_up,
                duration_second_scale_down,
            )
