from src.queue_controller.helpers import new_controller
from src.queue_controller.queueData import QueueData
from src.indexes.specialist_index.specialist_index import SpecialistIndex

def specialist_index_queue_data(specialist_index: SpecialistIndex, exchange_with_nodes = False) -> QueueData:
    """Sets QueueData state with specialist_index"""
    queue_data = QueueData()
    queue_data["specialist_index"] = specialist_index
    set_exchange_specialists(queue_data, exchange_with_nodes)
    return queue_data

def set_exchange_specialists(queue_data: QueueData, value: bool) -> QueueData:
    """load_or_store of exchange_specialists in QueueData"""
    queue_data["exchange_specialists"] = value
    return queue_data

def get_exchange_specialists(queue_data: QueueData) -> bool:
    """Returns state of exchange_specialists in QueueData"""
    return queue_data["exchange_specialists"]

def user_agent_queue_controller(
        user_id: str,
        specialist_index: SpecialistIndex,
        node_identity: str = None,
        exchange_specialists = None):

    async def agent_index_node(queue_data: QueueData):
        try:
            data_specialist_index: SpecialistIndex = queue_data["specialist_index"]
            if get_exchange_specialists(queue_data) and exchange_specialists is not None:
                await exchange_specialists(user_id, specialist_index, data_specialist_index, queue_data)
        except Exception as e:
            return e

    return new_controller(action=agent_index_node, identity=node_identity)


