"""Docker client for scaling containers."""
import os
import logging
import docker
from typing import List, Optional

logger = logging.getLogger(__name__)


class DockerClient:
    """Client for managing Docker containers via Docker API."""
    
    def __init__(self, base_url: Optional[str] = None):
        """Initialize Docker client.
        
        Args:
            base_url: Docker daemon URL (default: unix://var/run/docker.sock)
        """
        try:
            if base_url:
                self.client = docker.DockerClient(base_url=base_url)
            else:
                # Try to connect to Docker daemon
                # In Docker Compose, this will be the Docker socket
                self.client = docker.from_env()
            # Test connection
            self.client.ping()
            logger.info("Connected to Docker daemon")
        except Exception as e:
            logger.error(f"Failed to connect to Docker daemon: {e}")
            raise
    
    def get_service_containers(self, service_name: str) -> List[docker.models.containers.Container]:
        """Get all containers for a service.
        
        Args:
            service_name: Name of the service (container name prefix)
            
        Returns:
            List of container objects
        """
        try:
            # Get all containers (including stopped ones)
            all_containers = self.client.containers.list(all=True)
            
            # Filter by service name (containers are typically named like service_name_1, service_name_2, etc.)
            service_containers = [
                c for c in all_containers
                if c.name.startswith(service_name) or service_name in c.name
            ]
            
            return service_containers
        except Exception as e:
            logger.error(f"Failed to get service containers: {e}")
            return []
    
    def get_running_count(self, service_name: str) -> int:
        """Get the number of running containers for a service.
        
        Args:
            service_name: Name of the service
            
        Returns:
            Number of running containers
        """
        containers = self.get_service_containers(service_name)
        running = [c for c in containers if c.status == "running"]
        return len(running)
    
    def scale_service(self, service_name: str, target_replicas: int) -> bool:
        """Scale a service to target number of replicas.
        
        Note: This uses docker-compose scale via API, which requires
        the containers to be managed by docker-compose.
        
        Args:
            service_name: Name of the service to scale
            target_replicas: Target number of replicas
            
        Returns:
            True if scaling was successful, False otherwise
        """
        try:
            current_count = self.get_running_count(service_name)
            
            if current_count == target_replicas:
                logger.info(f"Service {service_name} already at {target_replicas} replicas")
                return True
            
            logger.info(f"Scaling {service_name} from {current_count} to {target_replicas} replicas")
            
            # Get containers
            containers = self.get_service_containers(service_name)
            
            if target_replicas > current_count:
                # Scale up: Start stopped containers or create new ones
                # For docker-compose, we'd typically use docker-compose scale command
                # But via API, we can start stopped containers
                stopped = [c for c in containers if c.status != "running"]
                needed = target_replicas - current_count
                
                for i, container in enumerate(stopped[:needed]):
                    container.start()
                    logger.info(f"Started container {container.name}")
                
                # If we need more, we can't create new ones via API easily
                # This would require docker-compose or docker swarm
                if needed > len(stopped):
                    logger.warning(
                        f"Cannot create new containers via API. "
                        f"Use 'docker-compose up --scale {service_name}={target_replicas}' "
                        f"or Docker Swarm for dynamic scaling."
                    )
                    return False
                    
            else:
                # Scale down: Stop excess containers
                running = [c for c in containers if c.status == "running"]
                excess = current_count - target_replicas
                
                for i, container in enumerate(running[:excess]):
                    container.stop(timeout=10)
                    logger.info(f"Stopped container {container.name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to scale service {service_name}: {e}")
            return False

