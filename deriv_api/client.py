"""
Deriv API Client - WebSocket Connection and Authentication
Handles all low-level communication with Deriv servers
"""

import json
import time
import logging
from typing import Dict, Optional, Callable, List
from datetime import datetime
import websocket
import threading
from queue import Queue, Empty
from enum import Enum

logger = logging.getLogger(__name__)


class DerivEnvironment(Enum):
    """Deriv API environments"""
    PRODUCTION = "wss://ws.deriv.com"
    DEMO = "wss://ws-demo.deriv.com"


class DerivClient:
    """
    Low-level Deriv WebSocket client
    Handles connection, authentication, and message routing
    """
    
    def __init__(self, 
                 api_token: str,
                 environment: DerivEnvironment = DerivEnvironment.DEMO,
                 app_id: Optional[str] = None,
                 timeout: int = 30):
        """
        Initialize Deriv API client
        
        Args:
            api_token: API token from Deriv dashboard
            environment: DEMO or PRODUCTION
            app_id: Deriv app ID (optional, for higher rate limits)
            timeout: Connection timeout in seconds
        """
        self.api_token = api_token
        self.environment = environment
        self.app_id = app_id
        self.timeout = timeout
        
        self.ws = None
        self.connected = False
        self.authenticated = False
        self.account_id = None
        
        # Message tracking
        self.request_id_counter = 0
        self.pending_requests = {}  # req_id -> response queue
        self.message_queue = Queue()
        
        # Callbacks for different message types
        self.callbacks = {
            'tick': [],
            'digits': [],
            'trade': [],
            'balance': [],
            'error': [],
            'connection': [],
        }
        
        # Connection state
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 2
        
    def connect(self) -> bool:
        """
        Establish WebSocket connection to Deriv
        
        Returns:
            True if successful, False otherwise
        """
        try:
            url = f"{self.environment.value}?app_id={self.app_id}" if self.app_id else self.environment.value
            
            logger.info(f"Connecting to {self.environment.name} ({self.environment.value})")
            
            self.ws = websocket.WebSocketApp(
                url,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
                on_open=self._on_open
            )
            
            # Run WebSocket in separate thread
            self.ws_thread = threading.Thread(
                target=self.ws.run_forever,
                kwargs={"ping_interval": 30, "ping_timeout": 10}
            )
            self.ws_thread.daemon = True
            self.ws_thread.start()
            
            # Wait for connection
            start_time = time.time()
            while not self.connected and (time.time() - start_time) < self.timeout:
                time.sleep(0.1)
            
            if not self.connected:
                logger.error("Connection timeout")
                return False
            
            logger.info("Connected to Deriv")
            return True
            
        except Exception as e:
            logger.error(f"Connection error: {e}")
            return False
    
    def authenticate(self) -> bool:
        """
        Authenticate with API token
        
        Returns:
            True if successful, False otherwise
        """
        try:
            response = self.send_request({
                'authorize': self.api_token
            })
            
            if response and response.get('authorize'):
                self.authenticated = True
                self.account_id = response['authorize'].get('account_list', [{}])[0].get('loginid')
                logger.info(f"Authenticated as account: {self.account_id}")
                self._trigger_callback('connection', {'status': 'authenticated'})
                return True
            else:
                error = response.get('error', {}).get('message', 'Unknown error') if response else 'No response'
                logger.error(f"Authentication failed: {error}")
                self._trigger_callback('error', {'message': f"Auth failed: {error}"})
                return False
                
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return False
    
    def send_request(self, request: Dict, wait_response: bool = True, timeout: int = 10) -> Optional[Dict]:
        """
        Send JSON request and optionally wait for response
        
        Args:
            request: Request dict (will be converted to JSON)
            wait_response: Wait for response from server
            timeout: Response timeout in seconds
            
        Returns:
            Response dict or None
        """
        try:
            self.request_id_counter += 1
            req_id = self.request_id_counter
            
            # Add req_id to request
            request['req_id'] = req_id
            
            # Create response queue if waiting
            if wait_response:
                self.pending_requests[req_id] = Queue()
            
            # Send request
            if not self.ws or not self.connected:
                logger.error("Not connected to Deriv")
                return None
            
            json_str = json.dumps(request)
            self.ws.send(json_str)
            
            logger.debug(f"Sent request {req_id}: {json_str[:200]}")
            
            # Wait for response if needed
            if wait_response:
                try:
                    response = self.pending_requests[req_id].get(timeout=timeout)
                    if req_id in self.pending_requests:
                        del self.pending_requests[req_id]
                    return response
                except Empty:
                    logger.error(f"Response timeout for request {req_id}")
                    if req_id in self.pending_requests:
                        del self.pending_requests[req_id]
                    return None
            
            return None
            
        except Exception as e:
            logger.error(f"Send request error: {e}")
            return None
    
    def _on_message(self, ws, message: str):
        """Handle incoming WebSocket message"""
        try:
            data = json.loads(message)
            req_id = data.get('req_id')
            
            # Route to pending request if applicable
            if req_id and req_id in self.pending_requests:
                self.pending_requests[req_id].put(data)
            
            # Route to callbacks based on message type
            if 'tick' in data:
                self._trigger_callback('tick', data['tick'])
            elif 'digit' in data:
                self._trigger_callback('digits', data)
            elif 'buy' in data or 'sell' in data:
                self._trigger_callback('trade', data)
            elif 'balance' in data:
                self._trigger_callback('balance', data)
            elif 'error' in data:
                self._trigger_callback('error', data.get('error', {}))
            
            logger.debug(f"Message received: {message[:200]}")
            
        except Exception as e:
            logger.error(f"Message handling error: {e}")
    
    def _on_error(self, ws, error):
        """Handle WebSocket error"""
        logger.error(f"WebSocket error: {error}")
        self._trigger_callback('error', {'message': str(error)})
    
    def _on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close"""
        logger.warning(f"WebSocket closed: {close_status_code} - {close_msg}")
        self.connected = False
        self.authenticated = False
        self._trigger_callback('connection', {'status': 'disconnected'})
        
        # Attempt reconnection
        self._attempt_reconnect()
    
    def _on_open(self, ws):
        """Handle WebSocket open"""
        logger.info("WebSocket opened")
        self.connected = True
        self.reconnect_attempts = 0
    
    def _attempt_reconnect(self):
        """Attempt to reconnect after disconnection"""
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            logger.error("Max reconnection attempts reached")
            self._trigger_callback('connection', {'status': 'failed'})
            return
        
        self.reconnect_attempts += 1
        wait_time = self.reconnect_delay * (2 ** (self.reconnect_attempts - 1))
        logger.info(f"Reconnecting in {wait_time}s (attempt {self.reconnect_attempts})")
        
        time.sleep(wait_time)
        
        if self.connect():
            if not self.authenticate():
                self._attempt_reconnect()
    
    def register_callback(self, message_type: str, callback: Callable):
        """
        Register callback for specific message type
        
        Args:
            message_type: 'tick', 'digits', 'trade', 'balance', 'error', 'connection'
            callback: Function to call with message data
        """
        if message_type in self.callbacks:
            self.callbacks[message_type].append(callback)
    
    def _trigger_callback(self, message_type: str, data: Dict):
        """Trigger all callbacks for a message type"""
        if message_type in self.callbacks:
            for callback in self.callbacks[message_type]:
                try:
                    callback(data)
                except Exception as e:
                    logger.error(f"Callback error: {e}")
    
    def close(self):
        """Close WebSocket connection"""
        if self.ws:
            self.ws.close()
            self.connected = False
            self.authenticated = False
            logger.info("Connection closed")


class DerivAPIException(Exception):
    """Deriv API exception"""
    pass
