from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class User(_message.Message):
    __slots__ = ("user_id",)
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    user_id: int
    def __init__(self, user_id: _Optional[int] = ...) -> None: ...

class Response_Code(_message.Message):
    __slots__ = ("response_code",)
    RESPONSE_CODE_FIELD_NUMBER: _ClassVar[int]
    response_code: int
    def __init__(self, response_code: _Optional[int] = ...) -> None: ...

class Request(_message.Message):
    __slots__ = ("jwt_token",)
    JWT_TOKEN_FIELD_NUMBER: _ClassVar[int]
    jwt_token: str
    def __init__(self, jwt_token: _Optional[str] = ...) -> None: ...

class Reply(_message.Message):
    __slots__ = ("user_id",)
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    user_id: int
    def __init__(self, user_id: _Optional[int] = ...) -> None: ...
