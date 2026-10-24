# SPDX-License-Identifier: 0BSD

"""Run with: uv run --extra anyio python -m unittest discover -s tests.

Each scenario runs in a fresh process to isolate AnyIO's import hooks and
bound hangs even when the shield loop prevents event-loop timers from firing.
"""

from __future__ import annotations

import asyncio
import subprocess  # ruff: ignore[suspicious-subprocess-import]
import sys
import unittest

from pathlib import Path


async def run_scenario(scenario, integration):
    if integration == "before":
        import anyio

    from aiologic.lowlevel import shield

    if integration == "after":
        # Exercise replacement after the plain implementation is initialized.
        await shield(asyncio.sleep(0))
        import anyio

    if integration == "none":
        assert "anyio" not in sys.modules

    started = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()
    protected = None

    async def body():
        nonlocal protected
        protected = asyncio.current_task()
        started.set()
        if scenario == "raise_cancelled":
            raise asyncio.CancelledError
        await release.wait()
        finished.set()
        if scenario == "caller_error":
            msg = "protected error"
            raise ValueError(msg)
        return 42

    async def caller():
        if scenario == "shutdown_control":
            return await body()
        if scenario == "cancel_future":
            future = asyncio.create_task(body())
            return await shield(future)
        return await shield(body)()

    if scenario == "anyio_scope":
        with anyio.CancelScope() as scope:
            scope.cancel()
            release.set()
            assert await shield(body)() == 42
            await anyio.lowlevel.checkpoint()
            msg = "scope cancellation was lost"
            raise AssertionError(msg)
        assert finished.is_set()
        return

    task = asyncio.create_task(caller())
    await started.wait()
    if scenario in {"shutdown", "shutdown_control"}:
        # asyncio.run() must cancel both the caller and the protected task.
        return

    if scenario in {"caller_cancel", "caller_error", "both_cancel"}:
        for _ in range(3):
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            assert not protected.done()
            assert not finished.is_set()

    if scenario in {"cancel_task", "cancel_future", "both_cancel"}:
        protected.cancel()
    elif scenario != "raise_cancelled":
        release.set()

    if scenario == "success":
        assert await task == 42
    elif scenario == "caller_error":
        try:
            await task
        except ValueError as exc:
            message = str(exc)
        else:
            msg = "protected error was lost"
            raise AssertionError(msg)
        assert message == "protected error"
    else:
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            msg = "cancellation was lost"
            raise AssertionError(msg)
        assert task.cancelled()
        if scenario == "caller_cancel":
            assert finished.is_set()
            assert protected.result() == 42
        else:
            assert protected.cancelled()


class AsyncioShieldTests(unittest.TestCase):
    def test_shield(self):
        for integration in ("none", "before", "after"):
            scenarios = [
                "shutdown_control",
                "shutdown",
                "raise_cancelled",
                "cancel_task",
                "cancel_future",
                "both_cancel",
                "caller_cancel",
                "caller_error",
                "success",
            ]
            if integration != "none":
                scenarios.append("anyio_scope")
            for scenario in scenarios:
                with self.subTest(integration=integration, scenario=scenario):
                    result = subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
                        [
                            sys.executable,
                            str(Path(__file__).resolve()),
                            scenario,
                            integration,
                        ],
                        capture_output=True,
                        text=True,
                        timeout=3,
                        check=False,
                    )
                    assert result.returncode == 0, result.stderr
                    assert result.stdout.strip() == "completed"


if __name__ == "__main__":
    if len(sys.argv) == 3:
        asyncio.run(run_scenario(*sys.argv[1:]))
        sys.stdout.write("completed\n")
    else:
        unittest.main()
