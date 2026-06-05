"""
Deriv API Integration Module
Professional WebSocket client for Deriv Digits trading
"""

from .client import DerivClient, DerivEnvironment, DerivAPIException
from .manager import DerivAccountManager
from .streamer import DigitStreamer, TickStreamer

__version__ = "1.0.0"
__all__ = [
    'DerivClient',
    'DerivEnvironment',
    'DerivAPIException',
    'DerivAccountManager',
    'DigitStreamer',
    'TickStreamer',
]
