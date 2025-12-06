"""Pub/Sub client for publishing events."""
import os
import json
import logging
from typing import Dict, Any
from google.cloud import pubsub_v1
from google.api_core import exceptions

logger = logging.getLogger(__name__)


class PubSubClient:
    """Client for publishing messages to Pub/Sub."""
    
    def __init__(self, project_id: str, topic_name: str):
        """Initialize Pub/Sub client.
        
        Args:
            project_id: GCP project ID
            topic_name: Name of the Pub/Sub topic
        """
        self.project_id = project_id
        self.topic_name = topic_name
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_name)
        
        # Ensure topic exists (for local emulator)
        self._ensure_topic_exists()
    
    def _ensure_topic_exists(self):
        """Ensure the topic exists, create if it doesn't."""
        try:
            self.publisher.get_topic(request={"topic": self.topic_path})
            logger.info(f"Topic {self.topic_path} already exists")
        except exceptions.NotFound:
            logger.info(f"Creating topic {self.topic_path}")
            self.publisher.create_topic(request={"name": self.topic_path})
    
    def publish(self, data: Dict[str, Any]) -> str:
        """Publish an event to Pub/Sub.
        
        Args:
            data: Event data as dictionary
            
        Returns:
            Message ID from Pub/Sub
            
        Raises:
            Exception: If publishing fails
        """
        try:
            # Serialize data to JSON
            message_data = json.dumps(data).encode('utf-8')
            
            # Publish message
            future = self.publisher.publish(
                self.topic_path,
                message_data,
                event_type=data.get('type', 'unknown')
            )
            
            message_id = future.result()
            logger.info(f"Published message {message_id} to {self.topic_path}")
            return message_id
            
        except Exception as e:
            logger.error(f"Failed to publish message: {e}", exc_info=True)
            raise

