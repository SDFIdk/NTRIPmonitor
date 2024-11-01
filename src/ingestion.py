#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""_summary_
This script is responsible for ingesting Real-Time Kinematic (RTK) correction data
in the RTCM format from an NTRIP caster and storing it in the UREGA database.

The script uses asynchronous programming to handle multiple NTRIP streams concurrently.
It connects to the NTRIP caster, receives the RTCM data, and then stores it in the
database.

The script uses the asyncpg library for PostgreSQL database operations and the
ntripstreams library for handling NTRIP streams and RTCM3 data.

The script can be run from the command line and takes arguments for the configuration
settings. The settings include the details for the NTRIP caster and the database.
"""

import asyncio
import json
import logging
import typing
import os
from argparse import ArgumentParser
from configparser import ConfigParser
import math
from multiprocessing import Lock, Manager, Process
from sys import exit
from time import time, sleep
from dotenv import load_dotenv

from databasehandling import DatabaseHandler
import asyncpg
from ntripclient import NtripClients
from settings import CasterSettings, DbSettings, MultiprocessingSettings
import decoderclasses
from loghandler import NtripLogHandler
from rtcm3 import Rtcm3

# from psycopg2 import Error, connect, extras
# from asyncpg import Connection, connect, exceptions as asyncpg_exceptions


def procSigint(signum: int, frame: typing.types.FrameType) -> None:
    logging.warning("Received SIGINT. Shutting down, Adjø!")
    exit(3)


def procSigterm(signum: int, frame: typing.types.FrameType) -> None:
    logging.warning("Received SIGTERM. Shutting down, Adjø!")
    exit(4)


async def watchdogHandler(casterSettingsDict: dict, mountPointList: list, tasks: dict, sharedEncoded: list, lock) -> None:
    while True:
        await asyncio.sleep(30)  # Sleep for 30 seconds

        runningTasks = asyncio.all_tasks()
        runningTasks = [task for task in runningTasks if task.get_name() != 'watchdog']
        runningTaskNames = [runningTask.get_name() for runningTask in runningTasks]

        if len(runningTasks) <= len(mountPointList):
            logging.debug(
                f"{runningTaskNames} tasks running, {mountPointList} wanted."
            )
            # For each desired task
            for wantedTask in mountPointList:
                casterId, mountpoint = wantedTask
                if mountpoint not in runningTaskNames:
                    casterSettings = casterSettingsDict[casterId]
                    tasks[mountpoint] = asyncio.create_task(
                        procRtcmStream(
                            casterSettings,
                            dbSettings,
                            mountpoint,
                            lock,
                            sharedEncoded,
                        ),
                        name=mountpoint,
                    )
                    logging.warning(f"{mountpoint} RTCM stream restarted.")

"""
async def dbInsertBatch(
    dbConnection,
    dbSettings: DbSettings,
    decodedFrames: list,
    decodedObs: None,
    tableList: None,
) -> None:
    try:
        rtcmPackageIds = await dbInsertRtcmInfoStoredBatch(dbConnection, decodedFrames)
        if dbSettings.storeObservations:
            await dbInsertObsInfoStoredBatch(
                dbConnection, decodedObs, rtcmPackageIds, tableList
            )
    except Exception as error:
        logging.error(f"Failed to insert and commit data to database with: {error}")
    return


async def dbInsertRtcmInfoStoredBatch(dbConnection, decodedFrames: list) -> list:
    # print("Inserting RTCM info")
    try:
        # Convert the data to JSON
        decodedFramesJson = json.dumps(decodedFrames)
        # Call the stored procedure
        rtcmPackageIds = await dbConnection.fetchval(
            "SELECT insert_rtcm_packages($1::json)", decodedFramesJson
        )
    except Exception as error:
        # Log an error message if the insertion fails
        logging.error(f"Failed to insert and commit rtcm data to database with: {error}")
    return rtcmPackageIds


async def dbInsertObsInfoStoredBatch(
    dbConnection, decodedObs: list, rtcmPackageIds: list, tableList: list
) -> list:
    for index, decodedObsFrame in enumerate(decodedObs):
        if decodedObsFrame is None:
            continue
        rtcmId = rtcmPackageIds[index]
        try:
            decodedObsFrame = [(rtcmId, *obsFrame) for obsFrame in decodedObsFrame]
        except Exception as error:
            logging.error(f"Error frame in iterable: {error} with frame {decodedObsFrame}")
        try:
            decodedObsFrameJson = json.dumps(decodedObsFrame)
            await dbConnection.execute(
                f"SELECT insert_{tableList[index]}_observations($1::json)",
                decodedObsFrameJson,
            )
        except Exception as error:
            logging.error(f"Failed to insert and commit obs data to database with: {error}")
    return None
"""


def clearList(sharedList):
    del sharedList[:]


async def decodeInsertConsumer(
    sharedEncoded,
    dbSettings,
    lock,
    fail: int = 0,
    retry: int = 3,
    checkInterval: float = 0.1,
):
    dBHandler = None
    rtcmMessage = Rtcm3()
    while True:
        if dbSettings:
            while True:
                try:
                    dBHandler = DatabaseHandler(dbSettings)
                    await dBHandler.initializePool()
                    break
                except Exception as error:
                    fail += 1
                    sleepTime = 5 * fail
                    if sleepTime > 300:
                        sleepTime = 300
                    logging.error(
                        "Failed to connect to database server: "
                        f"{dbSettings.database}@{dbSettings.host} "
                        f"with error: {error}"
                    )
                    logging.error(f"Will retry database connection in {sleepTime} seconds!")
                    await asyncio.sleep(sleepTime)
            logging.info(f"Connected to database: {dbSettings.database}@{dbSettings.host}.")
        try:
            while True:
                await asyncio.sleep(checkInterval)
                if sharedEncoded:
                    lock.acquire()
                    # Perform the list and clearList operations directly in the asyncio event loop
                    encodedFramesList = list(sharedEncoded)
                    clearList(sharedEncoded)
                    lock.release()
                    # Loop over all encoded frames and decode and insert the data
                    logging.debug(f"Decoding {len(encodedFramesList)} sets of frames with a total of {sum(len(frames) for frames in encodedFramesList)} frames.")
                    for encodedFrames in encodedFramesList:
                        try:
                            decodedFrames, decodedObs, tableList = await decoderclasses.Decoder.batchDecodeFrame(
                                encodedFrames, dbSettings.storeObservations, rtcmMessage
                            )
                            await dBHandler.dbInsertBatch(
                                dbSettings, decodedFrames, decodedObs, tableList
                            )
                        except Exception as error:
                            logging.error(f"An error occurred while batch decoding or batch inserting: {error}")
        except Exception as error:
            logging.error(f"An error occurred while decoding and appending {error}")
            lock.release()
        finally:
            if lock.locked():
                lock.release()
            if dBHandler:
                await dBHandler.closePool()

def mountpointSplitter(casterSettingsDict: dict, maxProcesses: int) -> list:
    """
    Used to split the mountpoints into chunks based on the number of processes to be run.
    Each chunk will run on its own core. 

    Args:
        casterSettingsDict (dict): Dictionary of Caster settings
        maxProcesses (int): maximum number of processes to be run

    Returns:
        list: lists of lists of tuples, where each tuple contains a mountpoint and its caster ID
    """
    try:
        totalMountpoints = sum(len(caster.mountpoints) for caster in casterSettingsDict.values())

        casterProcesses = []
        for casterId, caster in casterSettingsDict.items(): 
            processes = len(caster.mountpoints) / totalMountpoints * maxProcesses
            casterProcesses.append((casterId, processes))

        casters = list(casterSettingsDict.items())
        casters.sort(key=lambda caster: len(caster[1].mountpoints)) 
        groupedCasters = []
        group = []
        groupTotal = 0
        for i, (casterId, caster) in enumerate(casters):  
            if groupTotal + len(caster.mountpoints) / totalMountpoints < 1.0:
                group.append((casterId, caster))  
                groupTotal += len(caster.mountpoints) / totalMountpoints
            else:
                groupedCasters.append((group, max(1, groupTotal)))
                group = [(casterId, caster)]  
                groupTotal = len(caster.mountpoints) / totalMountpoints
        if group:
            groupedCasters.append((group, max(1, groupTotal)))

        decimalParts = [groupTotal - int(groupTotal) for group, groupTotal in groupedCasters]
        remainingProcesses = maxProcesses - sum(int(groupTotal) for group, groupTotal in groupedCasters)
        for i in sorted(range(len(decimalParts)), key=lambda i: decimalParts[i], reverse=True)[:remainingProcesses]:
            groupedCasters[i] = (groupedCasters[i][0], groupedCasters[i][1] + 1)

        chunks = []
        for group, groupTotal in groupedCasters:
            processes = int(groupTotal)
            for casterId, caster in group:  
                chunkSize = len(caster.mountpoints) // processes
                remainder = len(caster.mountpoints) % processes
                for i in range(processes):
                    end = (i + 1) * chunkSize + min(i, remainder)
                    chunk = [(casterId, mp) for mp in caster.mountpoints[i * chunkSize:end]]  
                    chunks.append(chunk)

        return chunks
    except Exception as e:
        logging.error(f"Failed to split mountpoints with Error: {e}")
        return []

async def appendToList(listToAppend, sharedList, lock):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lock.acquire)
    try:
        sharedList.append(listToAppend)
    except Exception as error:
        logging.error(f"An error occurred while appending to list: {error}")
    finally:
        lock.release()

async def periodicFrameAppender(encodedFrames, sharedEncoded, lock, mountPoint, checkInterval=0.05):
    while True:
        await asyncio.sleep(checkInterval)  # Wait for a short period to avoid hogging the CPU
        if encodedFrames and (time() - encodedFrames[-1]['timeStampInFrame']) > 0.05:
            logging.debug(f"{mountPoint}: {len(encodedFrames)} frames collected. append to shared.")
            try:
                await appendToList(encodedFrames[:], sharedEncoded, lock)
                encodedFrames.clear()
            except Exception as error:
                logging.error(f"An error occurred in periodic check for appending frames: {error}")
                

async def procRtcmStream(
    casterSettings: CasterSettings,
    dbSettings: DbSettings,
    mountPoint: str,
    lock,
    sharedEncoded,
    fail: int = 0,
    retry: int = 3,
) -> None:
    ntripclient = NtripClients()
    ntripLogger = NtripLogHandler() 
    ntripclient = await ntripLogger.requestStream(ntripclient, casterSettings, dbSettings, mountPoint, log_disconnect=False)
    encodedFrames = []
    
    asyncio.create_task(periodicFrameAppender(encodedFrames, sharedEncoded, lock, mountPoint))
    
    while True:
        try:
            frames_in_buffer, timeStamp = await ntripclient.getRtcmFrame()
            for rtcmFrame in frames_in_buffer:
                encodedFrames.append(
                    {
                        "frame": rtcmFrame,
                        "timeStampInFrame": timeStamp,
                        "messageSize": len(rtcmFrame),
                        "mountPoint": mountPoint,
                    }
                )
            if fail > 0:
                fail = 0
        except (ConnectionError, IOError, IndexError):
            ntripclient = await ntripLogger.requestStream(ntripclient, casterSettings, dbSettings, mountPoint, log_disconnect=True)

async def rtcmStreamTasks(
    casterSettingsDict: dict,
    dbSettings: DbSettings,
    mountPointList: list,
    sharedEncoded: list,
    lock,
) -> None:

    tasks = {}
    for casterId, mountpoint in mountPointList:
        casterSettings = casterSettingsDict[casterId]
        tasks[mountpoint] = asyncio.create_task(
            procRtcmStream(
                casterSettings,
                dbSettings,
                mountpoint,
                lock,
                sharedEncoded,
            ),
            name=mountpoint,
        )

    # Create the watchdog task
    tasks['watchdog'] = asyncio.create_task(watchdogHandler(casterSettingsDict, mountPointList, tasks, sharedEncoded, lock))

    # Wait for each task to complete
    await asyncio.gather(*tasks.values())


async def getMountpoints(
    casterSettings: CasterSettings, sleepTime: int = 30, fail: int = 0
) -> list[str]:
    """
    This function retrieves the list of mountpoints from the NTRIP caster.

    Parameters:
    casterSettings (CasterSettings): An instance of CasterSettings containing the caster URL.
    sleepTime (int): The time to wait before retrying if a connection error occurs.
    fail (int): The number of failed attempts to connect to the caster.

    Returns:
    list[str]: A list of mountpoints.
    """

    # Create an instance of NtripStream
    ntripclient = NtripClients()

    # Initialize an empty list to hold the mountpoints
    mountpoints = []

    # Try to request the source table from the caster
    logging.info('Requesting source table from caster for mountpoint information')
    try:
        sourceTable = await ntripclient.requestSourcetable(casterSettings.casterUrl)
    # If a connection error occurs, log an error message, wait, and retry
    except ConnectionError:
        fail += 1
        logging.error(
            f"{fail} failed attempt to NTRIP connect to {casterSettings.casterUrl}. "
            f"Will retry in {sleepTime} seconds."
        )
        asyncio.sleep(sleepTime)
    # If an unknown error occurs, log an error message and abort monitoring
    except Exception as error:
        logging.error(f"Unknown error : {error}")
    # If the source table is successfully retrieved, extract the mountpoints
    else:
        for row in sourceTable:
            sourceCols = row.split(sep=";")
            # If the row represents a stream (STR), add the mountpoint to the list
            if sourceCols[0] == "STR":
                mountpoints.append(sourceCols[1])
        return mountpoints

def reduceCasterDict(casterSettingsDict: dict, casterStatus: list) -> dict:
    reducedCasterSettingsDict = {}
    for caster, status in zip(casterSettingsDict.keys(), casterStatus):
        if status == 1:
            reducedCasterSettingsDict[caster] = casterSettingsDict[caster]
    return reducedCasterSettingsDict

async def downloadSourceTable(
    casterSettingsDict: dict, dbSettings: DbSettings, sleepTime: int = 10, fail: int = 0, retry: int = 3
) -> list[list[str]]:
    """ Reads the sourcetable and inserts relevant metadata for the mountpoints
    into the database. Done for efficient viewing and handling of metadata when
    visualizing the data.
    """
    # Create an instance of NtripStream
    ntripclient = NtripClients()
    mountpoints = []
    casterStatus = [0] * len(casterSettingsDict)  # Initialize the list with zeros

    for g, (caster, casterSettings) in enumerate(casterSettingsDict.items(), start=1):
        if caster == "Empty":
            logging.info(f"Skipping caster {g}: {caster}.")
            continue
        logging.info("Requesting source table from caster for mountpoint information")
        logging.info(f"name : {caster}")
        logging.info(f"Requesting source table from caster: {casterSettings.casterUrl}")
        while True:
            try:
                sourceTable = await ntripclient.requestSourcetable(casterSettings.casterUrl)
                logging.info(f"Source table received from {casterSettings.casterUrl}")
                for row in sourceTable:
                    sourceCols = row.split(sep=";")
                    # If the row represents a stream (STR), add the mountpoint to the list
                    if sourceCols[0] == "STR":
                        mountpoints.append([sourceCols[1], caster] + [sourceCols[i] for i in [2,3,8,9,10,13]])
                casterStatus[g-1] = 1  # Set the status to 1 if the caster is active
                logging.info(f"Found {len(mountpoints)} mountpoints in the source table.")
                break  # If the source table is successfully received, break the loop
            except Exception as error:
                fail += 1
                sleepTime = 5 * fail
                if sleepTime > 300:
                    sleepTime = 300
                logging.error(
                    f"{fail} failed attempt to NTRIP connect to {casterSettings.casterUrl}. "
                    "Will retry in f{sleepTime} seconds."
                )
                if fail > retry:  # If fail is greater than 5, break the loop
                    logging.info(f" Attempted to connect to {casterSettings.casterUrl} {retry} times without success. Skipping caster.")
                    break
    seenMountpoints = {}
    for mountpoint in mountpoints:
        if mountpoint[0] in seenMountpoints:
            logging.info("Warning : Duplicate mountpoint found.")
            logging.info(f"Duplicate mountpoint found: Name - {mountpoint[0]}, Caster - {mountpoint[1]}, Location - {mountpoint[2]}, Country Code - {mountpoint[4]}")
            logging.info(f"Original mountpoint: Name - {seenMountpoints[mountpoint[0]][0]}, Caster - {seenMountpoints[mountpoint[0]][1]}, Location - {seenMountpoints[mountpoint[0]][2]}, Country Code - {seenMountpoints[mountpoint[0]][4]}")
        else:
            seenMountpoints[mountpoint[0]] = mountpoint
    while True:
        try:
            dbConnection = await dbConnect(dbSettings)
            break  # If the operation is successful and fail is less than retry, break the loop
        # except (Exception, asyncpg.ConnectionError) as error:
        except Exception as error:
            fail += 1
            sleepTime = 5 * fail
            if sleepTime > 300:
                sleepTime = 300
            logging.error(
                "Failed to connect to database server: "
                f"{dbSettings.database}@{dbSettings.host} "
                f"with error: {error}"
            )
            if dbConnection:
                await dbConnection.close()
            await asyncio.sleep(sleepTime)
    logging.info(f"Connected to database: {dbSettings.database}@{dbSettings.host}.")
    mountpointJson = json.dumps(mountpoints)
    try:
        await dbConnection.execute(
            f"SELECT insert_sourcetable_constants($1::json)",
            mountpointJson,
        )
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
    logging.debug(f"Inserted {len(mountpoints)} mountpoints metadata into the database.")
    if dbConnection:
        await dbConnection.close()
    return casterStatus

def loadCasterSettings():
    load_dotenv()  # Load environment variables from .env file
    casterSettingsDict = {}

    # Iterate through environment variables to find caster settings
    for key, value in os.environ.items():
        if key.endswith("_CASTER_ID") and value != "Empty":
            casterInstance = CasterSettings()
            prefix = key.split("_")[0]  # Extract prefix (e.g., "1" from "1_CASTER_ID")
            caster_id = value  # The actual CASTER_ID value

            # Construct the keys for other settings based on the prefix
            caster_url_key = f"{prefix}_CASTER_URL"
            caster_user_key = f"{prefix}_CASTER_USER"
            caster_password_key = f"{prefix}_CASTER_PASSWORD"
            caster_mountpoint_key = f"{prefix}_CASTER_MOUNTPOINT"

            # Extract other settings using the constructed keys
            casterInstance.casterUrl = os.getenv(caster_url_key, "")
            casterInstance.user = os.getenv(caster_user_key, "")
            casterInstance.password = os.getenv(caster_password_key, "")
            casterInstance.mountpoints = list(map(str.strip, os.getenv(caster_mountpoint_key, "").split(",")))
            if casterInstance.mountpoints == [""]:
                casterInstance.mountpoints = []
            # Create a CasterSettings object and add it to the dictionary
            casterSettingsDict[caster_id] = casterInstance
    return casterSettingsDict


async def dbConnect(dbSettings: DbSettings):
    """
    Establishes a connection to the database using the provided settings.

    Parameters:
    dbSettings (DbSettings): An instance of DbSettings containing the database connection details.

    Returns:
    connection: A connection object that represents the database connection.
    """
    connection = await asyncpg.connect(
        user=dbSettings.user,
        password=dbSettings.password,
        host=dbSettings.host,
        port=dbSettings.port,
        database=dbSettings.database,
    )
    return connection

async def waitDbConnection(dbSettings: DbSettings):
    while True:
        try:
            dbConnection = await dbConnect(dbSettings)
            await dbConnection.close()
            sleep(1)
            break
        except Exception as error:
            logging.info("Database connection is not yet open. Waiting...")
            sleep(3)
    logging.info("Database is initialized. Initializing the monitor system.")

def initializationLogger(casterSettingsDict, dbSettings: DbSettings, processingSettings: MultiprocessingSettings):
    logMessages = [
        "----------------------------- Multiprocessing Settings -----------------------------",
        f"Multiprocessing active: {processingSettings.multiprocessingActive}",
    ]

    if processingSettings.multiprocessingActive:
        logMessages.extend([
            "  - Multiprocessing is active. Multi-core setup.",
            f"  - Maximum reading processes: {processingSettings.maxReaders}",
            f"  - Readers per Decoder: {processingSettings.readersPerDecoder}",
            f"  - Database insertion frequency (s): {processingSettings.clearCheck} s (WIP)",
            f"  - Processing Append frequency (s): {processingSettings.appendCheck} s (WIP)",
        ])
    else:
        logMessages.extend([
            "  - Multiprocessing is inactive. Single core setup.",
            "  - Maximum reading processes: Inactive",
            "  - Readers per Decoder: Inactive",
            f"  - Database insertion frequency (s): {processingSettings.clearCheck} s (WIP)",
            f"  - Processing Append frequency (s): {processingSettings.appendCheck} s (WIP)",
        ])

    logMessages.extend([
        "----------------------------- Caster Settings -----------------------------",
        f"Number of casters: {len(casterSettingsDict)}",
    ])

    for i, (caster, settings) in enumerate(casterSettingsDict.items(), start=1):
        logMessages.extend([
            "-------------------------------------",
            f"Caster {i}: {caster}",
            f"Caster URL: {settings.casterUrl}",
            f"Number of mountpoints : {len(settings.mountpoints)}",
        ])

    logMessages.extend([
        "----------------------------- Database Settings -----------------------------",
        f"Database : {dbSettings.database}",
        f"Host & Port : {dbSettings.host}:{dbSettings.port}",
        f"Store observations : {dbSettings.storeObservations}",
    ])
    
    if dbSettings.storeObservations:
        logMessages.extend([
            "----------------------- NOTE -----------------------",
            "Observables are set to be stored. Expect large data quantities.",
            "Recommended to use less frequent database and list append frequencies.",
        ])
    logMessages.extend(["---------------------------------------------------",
                        "Initializing the monitor system with above settings."])
    logging.info('\n'.join(logMessages))    
    return None

def runRtcmStreamTasks(casterSettingsDict: dict,dbSettings: DbSettings, mountpointChunk, sharedEncoded, lock):
    asyncio.run(rtcmStreamTasks(casterSettingsDict, dbSettings, mountpointChunk, sharedEncoded, lock))

def runDecodeInsertConsumer(sharedEncoded, dbSettings: DbSettings, lock):
    asyncio.run(decodeInsertConsumer(sharedEncoded, dbSettings, lock))

def RunMultiProcessing(casterSettingsDict: dict, dbSettings: DbSettings, processingSettings: MultiprocessingSettings):
    mountpointChunks = mountpointSplitter(casterSettingsDict,processingSettings.maxReaders)
    numberReaders = min(processingSettings.maxReaders, len(mountpointChunks))
    numberDecoders = math.ceil(numberReaders // processingSettings.readersPerDecoder)
    
    logging.info(f"Starting {numberReaders} readers and {numberDecoders} decoders.")
    with Manager() as manager:
        sharedEncodedList = [manager.list() for _ in range(numberDecoders)]
        lockList = [Lock() for _ in range(numberDecoders)]
        readingProcesses = []
        for i, mountpointChunk in enumerate(mountpointChunks):
            sharedEncoded = sharedEncodedList[i // processingSettings.readersPerDecoder]
            lock = lockList[i // processingSettings.readersPerDecoder]
            logging.info(f"Starting process {i+1} with {len(mountpointChunk)} mountpoints:"
                            f"{[t[1] for t in mountpointChunk]}")
            readingProcess = Process(target=runRtcmStreamTasks, args=(casterSettingsDict, dbSettings, mountpointChunk, sharedEncoded, lock))
            readingProcesses.append(readingProcess)

        decoderProcesses = []
        for sharedEncoded, lock in zip(sharedEncodedList, lockList):
            decoderProcess = Process(target=runDecodeInsertConsumer, args=(sharedEncoded, dbSettings, lock))
            decoderProcesses.append(decoderProcess)

        for readingProcess in readingProcesses:
            readingProcess.start()
        for decoderProcess in decoderProcesses:
            decoderProcess.start()
        for readingProcess in readingProcesses:
            readingProcess.join()
        for decoderProcess in decoderProcesses:
            decoderProcess.join()
    
def runSingleProcessing(casterSettingsDict: dict, dbSettings: DbSettings):
    # Code removed from current release. will be added back late June.
    return None

def main(casterSettingsDict: dict, dbSettings: DbSettings, processingSettings: MultiprocessingSettings):
    """
    The main function that sets up signal handlers and starts the RTCM stream tasks.

    Parameters:
    casterSettings (CasterSettings): An instance of CasterSettings containing the caster settings.
    dbSettings (DbSettings): An instance of DbSettings containing the database connection details.
    """
    while True:
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(waitDbConnection(dbSettings))
            sleep(5)
            break
        except Exception as error:
            sleep(3)

    initializationLogger(casterSettingsDict, dbSettings, processingSettings) # Setup statistics
    
    # Download the source table from casters.
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        casterStatus = loop.run_until_complete(downloadSourceTable(casterSettingsDict, dbSettings))
        sleep(3)
    except Exception as error:
        sleep(3)
    sleep(3)

    # reduce caster dictionary to contain only active casters
    casterSettingsDict = reduceCasterDict(casterSettingsDict, casterStatus)


    if processingSettings.multiprocessingActive:
        RunMultiProcessing(casterSettingsDict, dbSettings, processingSettings)
    else:
        runSingleProcessing(casterSettingsDict, dbSettings)
        # Single-core version pulled due to bugs after introducing new class handling. Re-introduced soon.



# This code is only executed if the script is run directly
if __name__ == "__main__":
    # Declare global variables
    global casterSettings
    global dbSettings
    global processingSettings
    global tasks
    tasks = {}

    # Set up argument parser
    parser = ArgumentParser()
    # Add command line arguments
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        help="Test connection to Ntripcaster without committing data to database.",
    )
    parser.add_argument(
        "-m",
        "--mountpoint",
        action="append",
        help="Name of mountpoint without leading / (e.g. MPT1). "
        + "Overrides mountpoint list in ingest.conf",
    )
    parser.add_argument(
        "-1",
        "--ntrip1",
        action="store_true",
        help="Use Ntrip 1 protocol.",
    )
    parser.add_argument(
        "-l",
        "--logfile",
        action="store",
        help="Log to file. Default output is terminal.",
    )
    parser.add_argument(
        "-v",
        "--verbosity",
        action="count",
        default=0,
        help="Increase verbosity level.",
    )
    # Parse command line arguments
    args = parser.parse_args()
    # Initialize config parser
    config = ConfigParser()

    # Initialize caster and database settings
    casterSettings = CasterSettings()
    dbSettings = DbSettings()
    processingSettings = MultiprocessingSettings()
    # Set verbosity level
    args.verbosity = 2
    # Set logging level based on verbosity
    logLevel = logging.ERROR
    if args.verbosity == 1:
        logLevel = logging.WARNING
    elif args.verbosity == 2:
        logLevel = logging.INFO
    elif args.verbosity > 2:
        logLevel = logging.DEBUG
    # Set up logging
    if args.logfile:
        logging.basicConfig(
            level=logLevel,
            filename=args.logfile,
            format="%(asctime)s;%(levelname)s;%(message)s",
        )
    else:
        logging.basicConfig(
            level=logLevel, format="%(asctime)s;%(levelname)s;%(message)s"
        )
    
    load_dotenv()
    casterSettingsDict = loadCasterSettings()
    # casterSettingsDict contains the CasterSettings instances for all casters
    dbSettings.host = os.getenv('DB_HOST')
    dbSettings.port = int(os.getenv('DB_PORT'))
    dbSettings.database = os.getenv('DB_NAME')
    dbSettings.user = os.getenv('DB_USER')
    dbSettings.password = os.getenv('DB_PASSWORD')
    dbSettings.storeObservations = os.getenv('DB_STORE_OBSERVATIONS') == 'True'

    processingSettings.multiprocessingActive = os.getenv('MULTIPROCESSING_ACTIVE') == 'True'
    processingSettings.maxReaders = int(os.getenv('MAX_READERS'))
    processingSettings.readersPerDecoder = int(os.getenv('READERS_PER_DECODER'))
    processingSettings.clearCheck = float(os.getenv('CLEAR_CHECK')) # Currently un-used. Will be used for clearing shared list.
    processingSettings.appendCheck = float(os.getenv('APPEND_CHECK')) # Currently un-used. Will be used for appending shared list.
    # Override mountpoints if specified in command line arguments
    if args.mountpoint:
        casterSettings.mountpoints = args.mountpoint
    # Get mountpoints from caster if not specified in config or command line arguments
    if casterSettings.mountpoints == []:
        casterSettings.mountpoints = asyncio.run(getMountpoints(casterSettings))
    # Log the mountpoints being used
    logging.debug(f"Using mountpoints: {casterSettings.mountpoints}")
    # If test mode is enabled, don't use database settings
    if args.test:
        dbSettings = None
    # Run the main function with the specified settings
    main(casterSettingsDict, dbSettings, processingSettings)