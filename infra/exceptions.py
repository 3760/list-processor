"""
基础设施层 - 异常类定义

所有模块的异常均继承自本模块定义的基础异常类，
便于统一捕获和日志记录。
"""


class CriticalError(Exception):
    """
    关键错误（Critical）
    表示应用无法继续运行的严重错误，如配置文件缺失、数据库连接失败等。
    捕获后应向用户展示弹窗并终止应用。
    """

    def __init__(self, message: str, detail: str = None):
        super().__init__(message)
        self.detail = detail

    def __str__(self):
        if self.detail:
            return f"{self.message}（{self.detail}）"
        return self.message


class DataQualityError(Exception):
    """
    数据质量错误
    表示输入数据存在质量问题（如格式错误、字段缺失、数据损坏等）。
    不终止应用，但需要记录并隔离输出。
    """

    def __init__(self, message: str, field: str = None, row_index: int = None):
        super().__init__(message)
        self.field = field
        self.row_index = row_index

    def __str__(self):
        parts = [self.args[0]]
        if self.field:
            parts.append(f"字段: {self.field}")
        if self.row_index is not None:
            parts.append(f"行号: {self.row_index}")
        return " | ".join(parts)


class ValidationError(Exception):
    """
    校验错误
    表示数据不符合业务规则或字段规范，如必填字段为空、字段值超出允许范围等。
    """

    def __init__(self, message: str, code: str = None):
        super().__init__(message)
        self.code = code

    def __str__(self):
        if self.code:
            return f"[{self.code}] {self.args[0]}"
        return self.args[0]


class ConfigError(Exception):
    """
    配置错误
    表示配置文件格式错误、缺少必要配置项、配置值类型不匹配等。
    """

    def __init__(self, message: str, config_key: str = None):
        super().__init__(message)
        self.config_key = config_key

    def __str__(self):
        if self.config_key:
            return f"配置项 '{self.config_key}': {self.args[0]}"
        return self.args[0]


class ProcessingError(Exception):
    """
    处理过程错误
    表示模块执行过程中出现的业务逻辑错误，如字典上码失败、去重逻辑异常等。
    """

    def __init__(self, message: str, module: str = None):
        super().__init__(message)
        self.module = module

    def __str__(self):
        if self.module:
            return f"[模块 {self.module}] {self.args[0]}"
        return self.args[0]
