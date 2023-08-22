#!/usr/bin/env python3

# Configuration for the Crisp environment
ENV = None

from .configuration import load_configuration
from .service import Service
from .servicemanager import ServiceManager

# Create a singleton holding the services
SERVICES = ServiceManager()
