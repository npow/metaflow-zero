"""Card exceptions."""
from metaflow.exception import MetaflowException


class CardNotPresentException(MetaflowException):
    headline = "Card not present"
