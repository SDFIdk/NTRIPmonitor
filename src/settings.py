#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dataclasses import dataclass, field


@dataclass
class DbSettings:
    host: str = None
    port: int = None
    database: str = None
    user: str = None
    password: str = None
    storeObservations: bool = None


@dataclass
class CasterSettings:
    casterUrl: str = None
    user: str = None
    password: str = None
    mountpoints: list[str] = field(default_factory=lambda: [])


@dataclass
class MultiprocessingSettings:
    multiprocessingActive: bool = None
    maxReaders: int = None
    readersPerDecoder: int = None
    clearCheck: float = None
    appendCheck: float = None
