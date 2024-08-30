import re
import asyncio
import cProfile
import io
import json
import logging
import typing
import os
from argparse import ArgumentParser
from configparser import ConfigParser
from datetime import datetime
from math import pow
from multiprocessing import Lock, Manager, Process
from signal import ITIMER_REAL, SIGALRM, SIGINT, SIGTERM, setitimer, signal
from sys import exit
from time import gmtime, strftime, time, sleep
from collections import defaultdict
from math import ceil
from dotenv import load_dotenv

import asyncpg
from ntripclient import NtripClients
from settings import CasterSettings, DbSettings, MultiprocessingSettings
import decoderclasses
# from psycopg2 import Error, connect, extras
# from asyncpg import Connection, connect, exceptions as asyncpg_exceptions