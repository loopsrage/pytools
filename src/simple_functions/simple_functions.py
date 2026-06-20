import uuid


def uuid5_from_args(namespace: str, *args):
    ns = uuid.UUID(namespace)
    return f"{uuid.uuid5(ns, "_".join([str(a) for a in args]))}"