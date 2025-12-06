"""Monitor Pub/Sub subscription metrics."""
import os
import logging
from typing import Optional
from google.cloud import pubsub_v1
from google.api_core import exceptions

logger = logging.getLogger(__name__)


class PubSubMonitor:
    """Monitor Pub/Sub subscription for queue lag."""
    
    def __init__(self, project_id: str, subscription_name: str):
        """Initialize Pub/Sub monitor.
        
        Args:
            project_id: GCP project ID
            subscription_name: Name of the Pub/Sub subscription
        """
        self.project_id = project_id
        self.subscription_name = subscription_name
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(
            project_id, subscription_name
        )
    
    def get_queue_lag(self) -> Optional[int]:
        """Get the current queue lag (num_undelivered_messages).
        
        Returns:
            Number of undelivered messages, or None if unavailable
        """
        try:
            # Get subscription
            subscription = self.subscriber.get_subscription(
                request={"subscription": self.subscription_path}
            )
            
            # Note: The actual num_undelivered_messages metric is available
            # through the Cloud Monitoring API, not directly from the subscription object.
            # For local emulator, we'll use a workaround by checking the subscription stats.
            # In production, you'd use the Cloud Monitoring API.
            
            # For emulator/local: We can't easily get this metric, so we'll return None
            # and the autoscaler will need to use an alternative method
            # In a real implementation, you'd call:
            # from google.cloud import monitoring_v3
            # client = monitoring_v3.MetricServiceClient()
            # ... query for num_undelivered_messages ...
            
            return None  # Will be implemented via monitoring API in production
            
        except exceptions.NotFound:
            logger.error(f"Subscription {self.subscription_path} not found")
            return None
        except Exception as e:
            logger.error(f"Failed to get queue lag: {e}")
            return None
    
    def get_subscription_info(self) -> Optional[dict]:
        """Get subscription information.
        
        Returns:
            Dictionary with subscription info, or None if unavailable
        """
        try:
            subscription = self.subscriber.get_subscription(
                request={"subscription": self.subscription_path}
            )
            return {
                "name": subscription.name,
                "topic": subscription.topic,
                "ack_deadline_seconds": subscription.ack_deadline_seconds,
            }
        except Exception as e:
            logger.error(f"Failed to get subscription info: {e}")
            return None

