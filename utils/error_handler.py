# utils/error_handler.py

class ValidationError(Exception):
    """Custom exception for validation errors in the OCR system."""
    
    def __init__(self, message: str, error_code: str = None, details: dict = None):
        super().__init__(message)
        self.error_code = error_code
        self.details = details or {}
        
    def __str__(self):
        base_msg = super().__str__()
        if self.error_code:
            return f"[{self.error_code}] {base_msg}"
        return base_msg


class ProcessingError(Exception):
    """Custom exception for OCR processing errors."""
    
    def __init__(self, message: str, step: str = None, details: dict = None):
        super().__init__(message)
        self.step = step
        self.details = details or {}
        
    def __str__(self):
        base_msg = super().__str__()
        if self.step:
            return f"[{self.step}] {base_msg}"
        return base_msg


class ConfigurationError(Exception):
    """Custom exception for configuration-related errors."""
    pass


class APIError(Exception):
    """Custom exception for external API errors."""
    
    def __init__(self, message: str, service: str = None, status_code: int = None, details: dict = None):
        super().__init__(message)
        self.service = service
        self.status_code = status_code
        self.details = details or {}
        
    def __str__(self):
        base_msg = super().__str__()
        if self.service and self.status_code:
            return f"[{self.service}:{self.status_code}] {base_msg}"
        elif self.service:
            return f"[{self.service}] {base_msg}"
        return base_msg