__all__ = (
    "NATSConnectionError",
    "NATSError",
    "NATSInvalidResponse",
    "NATSInvalidSchemeError",
    "NATSInvalidUrlError",
    "NATSTCPConnectionRequiredError",
    "NATSTLSConnectionRequiredError",
    "NATSUnexpectedResponse",
    "NATSRequestTimeoutError",
)


class NATSError(Exception):
    pass


class NATSUnexpectedResponse(NATSError):
    def __init__(self, line: bytes, *args, **kwargs) -> None:
        self.line = line
        super().__init__()


class NATSInvalidResponse(NATSError):
    def __init__(self, line: bytes, *args, **kwargs) -> None:
        self.line = line
        super().__init__()


class NATSConnectionError(NATSError):
    def __init__(self, line: str, *args, **kwargs) -> None:
        self.line = line
        super().__init__()


class NATSTCPConnectionRequiredError(NATSConnectionError):
    def __init__(self, line: str, *args, **kwargs) -> None:
        self.line = line
        super().__init__(line, *args, **kwargs)


class NATSTLSConnectionRequiredError(NATSConnectionError):
    def __init__(self, line: str, *args, **kwargs) -> None:
        self.line = line
        super().__init__(line, *args, **kwargs)


class NATSInvalidUrlError(NATSConnectionError):
    def __init__(self, line: str, *args, **kwargs) -> None:
        self.line = line
        super().__init__(line, *args, **kwargs)


class NATSInvalidSchemeError(NATSConnectionError):
    def __init__(self, line: str, *args, **kwargs) -> None:
        self.line = line
        super().__init__(line, *args, **kwargs)


class NATSRequestTimeoutError(NATSError):
    pass
