#!/usr/bin/env python3

import asyncio
from asyncio.tasks import ALL_COMPLETED, FIRST_COMPLETED, wait_for
import importlib
import os
import logging
from typing import Dict, Mapping, Union
import threading

# import traceback

from .service import Service


logger = logging.getLogger(__name__)


class ServiceManager(Mapping):
    def __init__(
        self,
    ) -> None:
        self._services: Dict[str, Service] = dict()
        self._thread = None
        self._loop = asyncio.get_event_loop()
        # In case of a background thread, a threading.Event
        self._name = None

    def __getitem__(self, k: str) -> Service:
        return self._services[k]

    def __iter__(self):
        return iter(self._services)

    def __len__(self) -> int:
        return len(self._services)

    def register(self, service=Union[str, Service], name: str = None) -> Service:
        """Add a service to the service manager.

        name: the name of the service.
        service: Specify either a service instance or a service class name. If a class name, an instance of that class will be instantiated.
        """
        if not isinstance(service, Service) and not isinstance(service, str):
            raise TypeError(f"Service {str(name)} is not of type str or Service")

        if name is None:
            name = service.__class__.__name__ if isinstance(service, Service) else service

        if name in self._services:
            raise KeyError(f"Duplicate service: {name}")

        if isinstance(service, str):
            service = self._create_service(service)

        self._services[name] = service

    def _create_service(self, service: str):
        components = service.split(".")
        module_name = ".".join(components[0:-1])
        if "[" in components[-1]:
            class_name, inst_name = components[-1].split("[")
            assert inst_name.endswith("]"), "Invalid format for 'pkg.mod.class[name]'"
            inst_name = inst_name[:-1]
        else:
            class_name = inst_name = components[-1]

        module = importlib.import_module(module_name)
        klass = getattr(module, class_name)
        svc_instance = klass(inst_name)
        return svc_instance

    async def _async_run(self):
        """Coroutine which run the registered services and waits on their completion.

        Will exit with the first exception or when all completed.
        """
        tasks = {}
        for name, svc in self._services.items():
            task = self._loop.create_task(svc.run())
            tasks[task] = name

        pending = tasks.keys()
        try:
            while pending:
                done, pending = await asyncio.wait(pending, return_when=FIRST_COMPLETED)
                for task in done:
                    if task.exception():
                        logger.error(f"Service {tasks[task]} terminated with an exception")
                        raise task.exception()
                    else:
                        logger.info(f"Service {tasks[task]} terminated with result: {task.result()}")
        finally:
            # Cancel the pending tasks when an exception was raised
            if pending:
                logger.info(f"Cancel the pending tasks")
                for task in pending:
                    task.cancel()
                await asyncio.wait(pending, return_when=ALL_COMPLETED)

    def _start(self):
        if not threading.current_thread() is threading.main_thread():
            # We assume the thread was created for the purpose of
            # running this ServiceManager instance and no event loop
            # exists yet.
            asyncio.set_event_loop(self._loop)

        self._task = self._loop.create_task(self._async_run())
        self._loop.run_until_complete(self._task)

    def start(self):
        """Run the registered services asynchronously, using the default thread loop.
        The function will return when one of the services terminates with an exception or
        when all services are terminated.

        """
        self._start()

    def stop(self):
        """Exit the process."""
        self._task.cancel()

    async def run(self):
        """Run the services."""
        await self._async_run()

    @property
    def loop(self):
        return self._loop

    @property
    def thread(self):
        return self._thread

    @property
    def name(self):
        return self._name

    @property
    def port(self):
        return self._port
