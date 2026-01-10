from lo_ingester import TransportPolicy


class SimpleRetryPolicy(TransportPolicy):
    """
    Simple exponential backoff retry policy.
    """

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay

    def before_request(self, req):
        # no-op
        return None

    def after_response(self, req, resp):
        # no-op
        return None

    def should_retry(self, exc, attempt) -> bool:
        """
        Retry only if:
        - exception exists
        - attempt < max_attempts
        """
        if exc is None:
            return False

        return attempt < self.max_attempts

    def backoff(self, attempt) -> float:
        """
        Exponential backoff.
        Return delay in seconds.
        """
        if attempt <= 1:
            return 0.0

        return self.base_delay * (2 ** (attempt - 2))
