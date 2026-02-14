from enum import Enum


class DecisionAction(str, Enum):
    AUTO_POST = "AUTO_POST"
    CS_REVIEW = "CS_REVIEW"
    ASK_CUSTOMER = "ASK_CUSTOMER"
    HOLD = "HOLD"


class IssueSeverity(str, Enum):
    INFO = "info"
    WARN = "warn"
    CRITICAL = "critical"
