import asyncio
import json
import os
import traceback
import unittest
from asyncio import CancelledError, QueueEmpty
from typing import Annotated, TypedDict

import pytest
from dotenv import load_dotenv
from langchain_ollama import ChatOllama, OllamaEmbeddings
from langgraph.graph import add_messages

from src.agent_queue_controller.agent_queue_controller import specialist_index_queue_data, user_agent_queue_controller
from src.fsspecc.base_fsspecfs.base_tool_agent import fs_agent
from src.fsspecc.imagefs.imagesfs_tool_agent import imagesfs_agent
from src.fsspecc.memfs.memfs_tool_agent import memfs_agent
from src.indexes.connection_index.connection_index import ConnectionIndex
from src.langchain_agent_ltm_stm.agent import CommandResult, base_agent
from src.queue_controller.helpers import new_controller, start_pipeline, stop_pipeline
from src.queue_controller.queueController import QueueController
from src.queue_controller.queueData import QueueData
from src.indexes.specialist_index.specialist_index import SpecialistIndex
from src.settings.helper import restore

load_dotenv()
restore(os.getenv("ENV_FILE"))
conn_index = ConnectionIndex()
llm = ChatOllama(model="functiongemma")
step_eval = ChatOllama(model="gemma3:27b")
emb = OllamaEmbeddings(model="bge-m3")

def debug_action(queue_data: QueueData):
    del queue_data["specialist_index"]
    jsd = json.dumps(queue_data.kwargs(), indent=2)
    print(jsd)

def exchange_pl(user_id, node_index, queue_index, tg, queue):

    def format_node(queue_data: QueueData):
        formatter = queue_index.agent_user_invoker(agent_name="request_formatter", user_id=user_id)[0]
        msg_for_president = formatter(queue_data.attribute_from_derivative("important_data", ""))
        msg_for_president = msg_for_president.result["messages"][-1].content
        queue_data["formatted_response"] = msg_for_president

    def node_invoke_node(queue_data: QueueData):
        formatted_response = queue_data.attribute_from_derivative("formatted_response", "")
        node_name = queue_data.attribute_from_derivative("node_name", "")
        node_invoker = node_index.agent_user_invoker(agent_name=node_name, user_id=user_id)[0]
        node_result: CommandResult = node_invoker(formatted_response)
        queue_data.set_attribute_derivative(f"{node_name}_result", node_result.model_dump(), "")

    def queue_invoke_node(queue_data: QueueData):
        formatted_response = queue_data.attribute_from_derivative("formatted_response", "")
        queue_name = queue_data.attribute_from_derivative("queue_name", "")
        queue_invoker = queue_index.agent_user_invoker(agent_name=queue_name, user_id=user_id)[0]
        queue_result: CommandResult = queue_invoker(formatted_response)
        queue_data.set_attribute_derivative(f"{queue_name}_result", queue_result.model_dump(), "")

    def accum_node(after: QueueController):

        def accumulate(queue_data: QueueData):
            queue_name = queue_data.attribute_from_derivative("queue_name", "")
            node_name = queue_data.attribute_from_derivative("node_name", "")

            queue_result = queue_data.attribute_from_derivative(f"{queue_name}_result", "")
            node_result = queue_data.attribute_from_derivative(f"{node_name}_result", "")

            if queue_result is not None and node_result is not None:
                message = {
                    "node_agent": node_result,
                    "queue_agent": queue_result
                }

                message_string = json.dumps(message)
                queue_data.set_attribute_derivative(f"{queue_name}_{node_name}_step_data", message_string, "")
                queue.put_nowait(after.enqueue(queue_data))
                print(queue_data.trace())

        qc = new_controller("accumulate", action=accumulate)
        return qc

    def analyze_step(queue_data: QueueData):
        message_string = queue_data.attribute_from_derivative("step_data", "")
        queue_name = queue_data.attribute_from_derivative("queue_name", "")
        node_name = queue_data.attribute_from_derivative("node_name", "")

        step_analyzer_invoker = queue_index.agent_user_invoker(agent_name="step_analyzer", user_id=user_id)[0]
        analysis_result: CommandResult = step_analyzer_invoker(message_string)
        queue_data[f"{node_name}_{queue_name}_step_analysis"] = analysis_result.model_dump()
        print(queue_data.trace())


    az_node = new_controller(identity="analyze_step", action=analyze_step)
    accm = accum_node(az_node)

    form_node = new_controller(identity="format_node", action=format_node)

    queue_agent_invoke = new_controller(identity="queue_invoke_node", action=queue_invoke_node)
    node_agent_invoke = new_controller(identity="node_invoke_node", action=node_invoke_node)

    queue_agent_invoke.set_next(accm)
    node_agent_invoke.set_next(accm)
    form_node.set_broadcast(
        {
            "queue_agent": queue_agent_invoke,
            "node_agent": node_agent_invoke
        }
    )
    return [form_node, accm, node_agent_invoke, queue_agent_invoke, accm, az_node]

async def exchange_specialist(user_id, node_index: SpecialistIndex, queue_index: SpecialistIndex, queue_data):
    agent_list = queue_index.agent_list
    pq = asyncio.Queue(maxsize=1024)
    sub_results = []

    async def listen_queue(queue: asyncio.Queue):
        try:
            while True:
                item = queue.get_nowait()
                if item is None:
                    continue

                sub_results.append(item)
                if item is not None:
                    sub_results.append(item)

                queue.task_done()
        except QueueEmpty:
            await asyncio.sleep(0)
            pass
        except CancelledError:
            raise

    try:
        async with asyncio.TaskGroup() as tg:
            cool_pl = exchange_pl(user_id, node_index, queue_index, tg, pq)
            task = tg.create_task(listen_queue(pq))
            start_pipeline(tg=tg, nodes=cool_pl)

            for node_name in node_index.agent_list:
                for queue_name in agent_list:
                    if node_name == queue_name or queue_name == "step_analyzer":
                        continue

                    qd = queue_data.copy_derivative(f"{node_name}_{queue_name}")
                    qd.set_attribute_derivative("node_name", node_name, "")
                    qd.set_attribute_derivative("queue_name", queue_name, "")
                    await cool_pl[0].enqueue(queue_data)

            await pq.put(None)
            await pq.join()
            task.cancel()
    except* Exception as e:
        traceback.print_exception(e)
        print(f"Agent transition failed: {e}")
        raise e
    finally:
        results = await asyncio.gather(*sub_results)
        task.cancel()
        await stop_pipeline(cool_pl)
        print(results)

class AgentRequestIdKeyValue(TypedDict):
    request_id: str
    messages: Annotated[list, add_messages]

class MyTestCase(unittest.IsolatedAsyncioTestCase):

    async def test_something(self):
        node_specialist_index_1 = SpecialistIndex("group_1")
        node_specialist_index_2 = SpecialistIndex("group_2")

        agent_group_1 = {
            "filesystem": await fs_agent(model=llm,emb=None),
            "memory": await memfs_agent(model=llm,emb=None),
            "images": await imagesfs_agent(model=llm,emb=None)
        }
        all_tools = []
        for agent in agent_group_1.values():
            for name, tool in agent.tools:
                report = [
                    f"Tool Name: {tool.name}",
                    f"Description: {tool.description}",
                    f"Inputs Required: {tool.args}", # Returns a dict of types/descriptions
                    f"Custom Metadata: {tool.metadata}" # If defined during tool creation
                ]
                all_tools.append("\n".join(report))


        node_specialist_index_2.register_agents(agent_group_1)
        request_formatter = await base_agent(
            schema=AgentRequestIdKeyValue,
            model=step_eval,
            emb=emb,
            system_prompt=["You are a request formatting model that prepares user prompts for use with functiongemma."
                           "You are to include only structured data in response. A list of available tools is as follows:",
                           f"[{','.join(all_tools)}]"])

        step_analyzer = await base_agent(
            schema=AgentRequestIdKeyValue,
            model=step_eval,
            emb=emb,
            system_prompt=["You are an analysis model to analyse responses of other models provided in json"])
        agent_group_2 = {
            "request_formatter": request_formatter,
            "step_analyzer": step_analyzer,
        }

        node_specialist_index_1.register_agents(agent_group_1)
        node_specialist_index_2.register_agents(agent_group_2)

        mock_controller = new_controller("debug", action=debug_action)

        qc = user_agent_queue_controller("user", node_specialist_index_1, "filesystem", exchange_specialist)
        qc.set_next(mock_controller)

        pl = [qc, mock_controller]

        async with asyncio.TaskGroup() as tg:
            try:
                start_pipeline(tg=tg, nodes=pl)
                for j in range(0,1):
                    qd = specialist_index_queue_data(node_specialist_index_2, True)
                    qd["important_data"] = f"Save {j} barrels of oil for me. If it does not exist create it"
                    await qc.enqueue(qd)
            except ExceptionGroup as eg:
                pytest.fail(f"Pipeline node failed: {eg}")
            finally:
                await stop_pipeline(nodes=pl)



if __name__ == '__main__':
    unittest.main()
