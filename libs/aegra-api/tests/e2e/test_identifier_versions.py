from contextlib import AsyncExitStack
from uuid import UUID

import pytest
from httpx import AsyncClient

from aegra_api.settings import settings
from tests.e2e._utils import get_e2e_client


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_identifiers_for_new_assistants_threads_runs_and_crons() -> None:
    client = get_e2e_client()
    graph_id = "cron_example"

    async with AsyncExitStack() as cleanup, AsyncClient(base_url=settings.app.SERVER_URL, timeout=30) as http:
        assistant = await client.assistants.create(graph_id=graph_id, config={"tags": ["id-versions"]})
        assistant_id = assistant["assistant_id"]
        cleanup.push_async_callback(client.assistants.delete, assistant_id)
        thread = await client.threads.create()
        thread_id = thread["thread_id"]
        cleanup.push_async_callback(client.threads.delete, thread_id)

        cron = await http.post(
            "/runs/crons",
            json={
                "assistant_id": assistant_id,
                "schedule": "0 0 * * *",
                "enabled": False,
                "input": {"messages": []},
            },
        )
        cron.raise_for_status()
        cron_id = cron.json()["cron_id"]
        cleanup.push_async_callback(client.crons.delete, cron_id)

        assert UUID(assistant_id).version == 5
        assert UUID(thread_id).version == 7
        assert UUID(cron_id).version == 7

        run = await client.runs.create(thread_id, assistant_id, input={"messages": []})
        assert UUID(run["run_id"]).version == 7
        state = await client.runs.join(thread_id, run["run_id"])
        assert state["messages"][-1]["content"].startswith("Tick #1")


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_request_ids_generate_v7_and_preserve_valid_incoming_values() -> None:
    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30) as http:
        response = await http.get("/live")
        response.raise_for_status()
        generated_id = response.headers["x-request-id"]
        assert UUID(generated_id).version == 7

        legacy_id = "6c0310a8-045e-49d5-9131-b50151386a2f"
        for incoming in (legacy_id, generated_id):
            response = await http.get("/live", headers={"x-request-id": incoming})
            response.raise_for_status()
            assert response.headers["x-request-id"] == incoming
