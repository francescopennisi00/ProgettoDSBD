from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class User(_message.Message):
    __slots__ = ["user_id"]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    user_id: int
    def __init__(self, user_id: _Optional[int] = ...) -> None: ...

class JsonEliminatedData(_message.Message):
    __slots__ = ["json_eliminated_data"]
    JSON_ELIMINATED_DATA_FIELD_NUMBER: _ClassVar[int]
    json_eliminated_data: str
    def __init__(self, json_eliminated_data: _Optional[str] = ...) -> None: ...

class Request(_message.Message):
    __slots__ = ["jwt_token"]
    JWT_TOKEN_FIELD_NUMBER: _ClassVar[int]
    jwt_token: str
    def __init__(self, jwt_token: _Optional[str] = ...) -> None: ...

class Reply(_message.Message):
    __slots__ = ["user_id"]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    user_id: int
    def __init__(self, user_id: _Optional[int] = ...) -> None: ...

class ResponseCode(_message.Message):
    __slots__ = ["code"]
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: int
    def __init__(self, code: _Optional[int] = ...) -> None: ...
