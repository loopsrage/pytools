import hashlib
from typing import List

from langchain_agent_ltm_stm.agent import run_command, run_commands, arun_command, arun_commands
from thread_safe.index import Index


def get_global_index(name_identifier):
    full_hash = hashlib.sha256(name_identifier.encode('utf-8')).digest()
    truncated_bytes = full_hash[:16]
    global_index = int.from_bytes(truncated_bytes, byteorder='big')
    return global_index

class SpecialistIndex:
    _index: Index = None
    _namespace: str = None

    # Key for retrieving the agent name based on the reverse index (global_index)
    _reverse_index_key: str = None

    # Key for agent name passed to self.register with string_to_int(name)
    _agent_index_key: str = None

    def __init__(self, namespace: str):
        self._index = Index()
        self._namespace = namespace
        self._reverse_index_key = f"{namespace}_reverse_index"
        self._index.new(namespace)
        self._index.new(self._reverse_index_key)

    def register_agent(self, name, agent):
        reverse_index_key = get_global_index(name)
        self._index.store_in_index(self.namespace, name, agent)
        self._index.store_in_index(self._reverse_index_key, reverse_index_key, name)
        self._index.store_in_index(self._agent_index_key, name, reverse_index_key)

    def register_agents(self, agents):
        for name, agent in agents.items():
            self.register_agent(name, agent)

    def agent(self, name: str):
        return self._index.load_from_index(self.namespace, name)

    def agent_name_from_reverse_index(self, reverse_index: int) -> str:
        return self._index.load_from_index(self._reverse_index_key, reverse_index)

    def agent_reverse_index(self, name: str) -> int:
        return self._index.load_from_index(self._agent_index_key, name)

    @property
    def range_agents(self):
        for name, agent in self._index.range_index(self.namespace):
            yield name, agent

    @property
    def agent_list(self):
        return [name for name, _ in list(self.range_agents)]

    @property
    def agent_dict(self):
        return {i:v for i, v in self.range_agents}

    def agent_ri_dict(self):
        return {self.agent_reverse_index(i): v for i, v in self.range_agents}

    @property
    def namespace(self):
        return self._namespace

    def agent_user_invoker(self, agent_name, user_id):

        def invoke(query: str | List[str]):
            try:
                _agent = self.agent(agent_name)
                if isinstance(query, str):
                    return run_command(_agent, user_id, self.namespace, query)
                return [i for i in invoke_commands(query)]
            except KeyError as e:
                e.add_note(f"agent {agent_name} does not exist in registry")
                raise e
            except Exception:
                raise

        def invoke_commands(commands: List[str]):
            try:
                _agent = self.agent(agent_name)
                for i in run_commands(_agent, user_id, self.namespace, commands):
                    yield i
            except KeyError as e:
                e.add_note(f"agent {agent_name} does not exist in registry")
                raise e
            except Exception:
                raise
        return invoke, invoke_commands

    def async_agent_user_invoker(self, agent_name, user_id):

        async def ainvoke(query: str | List[str]):
            try:
                _agent = self.agent(agent_name)
                if isinstance(query, str):
                    return await arun_command(_agent, user_id, self.namespace, query)
                return [i async for i in ainvoke_commands(query)]
            except KeyError as e:
                e.add_note(f"agent {agent_name} does not exist in registry")
                raise e
            except Exception:
                raise

        async def ainvoke_commands(commands: List[str]):
            try:
                _agent = self.agent(agent_name)
                async for i in arun_commands(_agent, user_id, self.namespace, commands):
                    yield i
            except KeyError as e:
                e.add_note(f"agent {agent_name} does not exist in registry")
                raise e
            except Exception:
                raise

        return ainvoke, ainvoke_commands
