"""投递侧可区分的拒绝原因。"""


class DispatchError(Exception):
    """所有投递拒绝的基类。"""


class InvalidRequestError(DispatchError):
    """请求不合法（缺业务键/分组名、载荷为空等）。"""


class KeyOccupiedError(DispatchError):
    """业务键已被一个尚未完成的任务占用（重复投递）。"""

    def __init__(self, key: str, group: str):
        self.key = key
        self.group = group
        super().__init__(f"key occupied: group={group!r} key={key!r}")


class PayloadConflictError(DispatchError):
    """同一业务键已有任务，但本次载荷内容与已存内容不一致。"""

    def __init__(self, key: str, group: str):
        self.key = key
        self.group = group
        super().__init__(f"payload conflict: group={group!r} key={key!r}")
