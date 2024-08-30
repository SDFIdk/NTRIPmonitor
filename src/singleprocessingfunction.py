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
import cProfile
import io
import json
import logging
import pstats
import typing
from argparse import ArgumentParser
from configparser import ConfigParser
from datetime import datetime
from math import pow
from signal import ITIMER_REAL, SIGALRM, SIGINT, SIGTERM, setitimer, signal
from sys import exit
from time import gmtime, strftime, time

import asyncpg
from ntripstreams import NtripStream, Rtcm3
from settings import CasterSettings, DbSettings

# from psycopg2 import Error, connect, extras
# from asyncpg import Connection, connect, exceptions as asyncpg_exceptions


def procSigint(signum: int, frame: typing.types.FrameType) -> None:
    """
    Signal handler for interrupt (SIGINT).

    This function is triggered when an interrupt signal (SIGINT) is received,
    typically as a result of the user pressing Ctrl+C. When this happens,
    the function logs a warning and shuts down the program.

    Parameters
    ----------
    signum : int
        The signal number that triggered this handler. This is system-specific and
        not intended for direct user calls.
    frame : FrameType
        The current stack frame at the time the signal was triggered. This is
        system-specific and not intended for direct user calls.

    Returns
    -------
    None
    """
    logging.warning("Received SIGINT. Shutting down, Adjø!")
    exit(3)


def procSigterm(signum: int, frame: typing.types.FrameType) -> None:
    """
    Signal handler for terminate (SIGTERM).

    This function is triggered when a terminate signal (SIGTERM) is received,
    typically as a result of the user or another process requesting the program to
    terminate. When this happens, the function logs a warning and shuts down the program.

    Parameters
    ----------
    signum : int
        The signal number that triggered this handler. This is system-specific and
        not intended for direct user calls.
    frame : FrameType
        The current stack frame at the time the signal was triggered. This is
        system-specific and not intended for direct user calls.

    Returns
    -------
    None
    """
    logging.warning("Received SIGTERM. Shutting down, Adjø!")
    exit(4)


def watchdogHandler(signum: int, frame: typing.types.FrameType) -> None:
    """
    Signal handler for the watchdog.

    This function is triggered when the watchdog timer expires. It checks the currently
    running asyncio tasks against the desired tasks specified in the casterSettings.
    If any of the desired tasks are not currently running, it restarts them.

    Parameters
    ----------
    signum : int
        The signal number that triggered this handler. This is system-specific and
        not intended for direct user calls.
    frame : FrameType
        The current stack frame at the time the signal was triggered. This is
        system-specific and not intended for direct user calls.

    Returns
    -------
    None

    Notes
    -----
    This function is designed to be used with a watchdog timer, which should be set
    to trigger at regular intervals. If this function finds that a desired task is
    not running, it will restart the task and log a warning.
    """

    # Get all currently running asyncio tasks
    runningTasks = asyncio.all_tasks()
    # Extract the names of the running tasks
    runningTaskNames = [runningTask.get_name() for runningTask in runningTasks]
    # If the number of running tasks (excluding the current task) is less than or equal to the number of desired tasks
    if len(runningTasks) - 1 <= len(casterSettings.mountpoints):
        # Log the names of the running tasks and the desired tasks
        logging.debug(
            f"{runningTaskNames} tasks running, {casterSettings.mountpoints} wanted."
        )
        # Initialize a list to hold the names of the desired tasks that are not currently running
        wantedTaskNames = []
        # For each desired task
        for wantedTask in casterSettings.mountpoints:
            # If the task is not currently running
            if wantedTask not in runningTaskNames:
                # Add the task name to the list of wanted tasks
                wantedTaskNames.append(wantedTask)
                # Create a new task for the desired task
                tasks[wantedTask] = asyncio.create_task(
                    procRtcmStream(
                        casterSettings,
                        wantedTask,
                        dbSettings,
                    ),
                    name=wantedTask,
                )
                # Log a warning that the task has been restarted
                logging.warning(f"{wantedTask} RTCM stream restarted.")
    # Return from the function
    return


def gnssEpochStr(messageType: int, obsEpoch: float, Type: int) -> str:
    """
    Constructs a SQL suited date/time string from a GNSS observation epoch.

    This function takes a GNSS observation epoch and converts it into a SQL
    compatible date/time string. The date is adopted from the current computer
    date. To align the GNSS data with UTC date, the GNSS epoch modulus 1 day is
    compared with computer time modulus 1 day.

    Parameters
    ----------
    messageType : int
        The RTCM message type. This is used to determine if a time correction
        should be applied to the observation time.
    obsEpoch : float
        The observation epoch in seconds using the GNSS constellation timesystem.

    Returns
    -------
    str
        A SQL compatible date/time string representing the observation epoch.

    Notes
    -----
    This function assumes that the observation epoch is in seconds since the
    beginning of the GNSS timesystem. It also assumes that the messageType is
    an integer representing a valid RTCM message type.
    """
    # Get the current time in seconds since the epoch
    now = time()
    # Calculate the number of seconds that have passed today
    nowSecOfDay = now % 86400
    # Calculate the number of seconds that have passed since the epoch, excluding today
    nowSecOfDate = int(now - nowSecOfDay)

    # Calculate the number of seconds that have passed on the day of the observation
    obsSecOfDay = int(obsEpoch % 86400)
    # Calculate the microseconds part of the observation epoch
    us = int(obsEpoch % 1 * 1000000)

    # If the observation time is more than 5 hours behind the current time, assume it's from the next day
    if (obsSecOfDay - nowSecOfDay) < -5 * 3600:
        obsTime = nowSecOfDate + obsSecOfDay + 86400
    else:
        obsTime = nowSecOfDate + obsSecOfDay
    # If the message type indicates that the observation is from a GLONASS satellite, adjust the time by -3 hours
    if (messageType >= 1009 and messageType <= 1012) or (
        messageType >= 1081 and messageType <= 1087
    ):
        obsTime = obsTime - 3 * 3600
    # Log the message type, current time, observation time, and time difference
    logging.debug(
        f"Msg:{messageType} CPU:{strftime(f'%Y-%m-%d %H:%M:%S.{us} z', gmtime(now))} "
        f"obsTime:{strftime(f'%Y-%m-%d %H:%M:%S.{us} z', gmtime(obsTime))} "
        f"timeDiff:{obsSecOfDay - nowSecOfDay}"
    )
    # Convert the observation time to a string in the format "YYYY-MM-DD HH:MM:SS.us Z"
    if Type == 1:  # With stored procedure
        epochStr = strftime(f"%Y-%m-%d %H:%M:%S.{us} z", gmtime(obsTime))
    else:  # Without stored procedure
        epochStr = datetime.fromtimestamp(obsTime).replace(microsecond=us)
    return epochStr


async def batchDecodeFrame(encodedFrames: list, mountPoint: str, storeObsCheck) -> list:
    """
    Does not handle GnssOBS yet. Only RTCM info.
    """
    decodedFrames = []
    decodedObs = []
    tableList = []
    rtcmMessage = Rtcm3()
    if storeObsCheck:
        satType_table_dict = {
            range(1001, 1005): ("G", "gps"),
            range(1071, 1078): ("G", "gps"),
            range(1009, 1013): ("R", "glonass"),
            range(1081, 1088): ("R", "glonass"),
            range(1091, 1098): ("E", "galileo"),
            range(1101, 1108): ("S", "sbas"),
            range(1111, 1118): ("J", "qzss"),
            range(1121, 1128): ("C", "beidou"),
        }

    for frame, timeStampInFrame, messageSize in encodedFrames:
        try:
            messageType, data = rtcmMessage.decodeRtcmFrame(frame)
            logging.debug(
                f"{mountPoint}: RTCM message #: {messageType}"
                f" '{rtcmMessage.messageDescription(messageType)}'."
            )
            if messageType in [1127, 1097, 1087, 1077]:
                satCount = len(data[1])
                obsEpochStr = gnssEpochStr(messageType, data[0][2] / 1000.0, 1)
            else:
                satCount = None
                obsEpochStr = None

            decodedFrames.append(
                (
                    mountPoint,
                    # datetime.fromtimestamp(timeStampInFrame).replace(microsecond=int((timeStampInFrame % 1)
                    # * 1e6)),
                    # Without Procedure
                    # strftime(
                    #     f"%Y-%m-%d %H:%M:%S.{int(timeStampInFrame % 1 * 1000000)} z",
                    #     gmtime(timeStampInFrame),
                    # ),
                    strftime(
                        f"%Y-%m-%d %H:%M:%S.{int(timeStampInFrame % 1 * 1000000):06}",
                        gmtime(timeStampInFrame),
                    ),
                    obsEpochStr,
                    messageType,
                    messageSize,
                    satCount,   
                    # datetime.fromtimestamp(time()).replace(microsecond=int((time() % 1) * 1e6)), # Without procedure
                    # strftime(
                    #     f"%Y-%m-%d %H:%M:%S.{int(time() % 1 * 1000000)} z",
                    #     gmtime(time()),
                    # ),
                    # strftime(
                    #     f"%Y-%m-%d %H:%M:%S.{int(timeStampInFrame % 1 * 1000000):06}",
                    #     gmtime(timeStampInFrame),
                    # ),
                )
            )
            if (storeObsCheck) and (messageType in [1127, 1097, 1087, 1077]):
                # print("Entered observables decoding")
                for key in satType_table_dict:
                    if messageType in key:
                        satType, table = satType_table_dict[key]
                        tableList.append(table)
                        break
                if messageType >= 1071 and messageType <= 1127:
                    obsEpochStr = gnssEpochStr(messageType, data[0][2] / 1000.0, 1)
                    satSignals = rtcmMessage.msmSignalTypes(messageType, data[0][10])
                    signalCount = len(satSignals)
                    availSatMask = str(data[0][9])
                    satId = [
                        f"{satType}{id + 1:02d}"
                        for id in range(64)
                        if availSatMask[id] == "1"
                    ]

                    messageType_mod = messageType % 10
                    if messageType_mod == 5:
                        codeFineScaling = pow(2, -24)
                        phaseFineScaling = pow(2, -29)
                        snrScaling = 1
                    elif messageType_mod == 7:
                        codeFineScaling = pow(2, -29)
                        phaseFineScaling = pow(2, -31)
                        snrScaling = pow(2, -4)
                    availObsNo = 0
                    availObsMask = str(data[0][11])
                    allObs = []

                    for satNo, sat in enumerate(data[1]):
                        satRoughRange = sat[0] + sat[2] / 1024.0
                        satRoughRangeRate = sat[3]

                        for signalNo, satSignal in enumerate(satSignals):
                            if availObsMask[satNo * signalCount + signalNo] == "1":
                                obsCode = (
                                    satRoughRange
                                    + data[2][availObsNo][0] * codeFineScaling
                                )
                                obsPhase = (
                                    satRoughRange
                                    + data[2][availObsNo][1] * phaseFineScaling
                                )
                                obsLockTimeIndicator = data[2][availObsNo][2]
                                obsSnr = data[2][availObsNo][4] * snrScaling
                                obsDoppler = (
                                    satRoughRangeRate + data[2][availObsNo][5] * 0.0001
                                )
                                allObs.append(
                                    (
                                        mountPoint,
                                        obsEpochStr,
                                        messageType,
                                        satId[satNo],
                                        satSignal,
                                        obsCode,
                                        obsPhase,
                                        obsDoppler,
                                        obsSnr,
                                        obsLockTimeIndicator,
                                    )
                                )
                                availObsNo += 1
                    decodedObs.append(allObs)
                    # print("Finished observables")
        except Exception as error:
            logging.info(f"Failed to decode RTCM frame. {error}")

    return decodedFrames, decodedObs, tableList


async def dbInsertBatch(
    dbConnection,
    dbSettings: DbSettings,
    decodedFrames: list,
    decodedObs: None,
    tableList: None,
) -> None:
    # Initialize lists for the values to be inserted
    # rtcmPackageIds = await dbInsertRtcmInfoBatch(dbConnection, decodedFrames)
    # print("inserting RTCM info batch")
    rtcmPackageIds = await dbInsertRtcmInfoStoredBatch(dbConnection, decodedFrames)
    # print("batch RTCM info inserted")
    if dbSettings.storeObservations:
        await dbInsertObsInfoStoredBatch(
            dbConnection, decodedObs, rtcmPackageIds, tableList
        )
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
        # print(f"rtcmpackageIds : {rtcmPackageIds}")
    except Exception as error:
        # Log an error message if the insertion fails
        logging.error(f"Failed to insert and commit RTCM data to database with: {error}")
    return rtcmPackageIds


async def dbInsertObsInfoStoredBatch(
    dbConnection, decodedObs: list, rtcmPackageIds: list, tableList: list
) -> list:
    # print("Inserting Observables")
    # print(rtcmPackageIds)
    for index, decodedObsFrame in enumerate(
        decodedObs
    ):  # Redo this code to use stored procedures
        # print(rtcmPackageIds,index)
        rtcmId = rtcmPackageIds[index]
        decodedObsFrame = [(rtcmId, *obsFrame) for obsFrame in decodedObsFrame]
        try:
            # Convert the data to JSON
            decodedObsFrameJson = json.dumps(decodedObsFrame)
            # Call the appropriate stored procedure based on the table name
            await dbConnection.execute(
                f"SELECT insert_{tableList[index]}_observations($1::json)",
                decodedObsFrameJson,
            )
        except Exception as error:
            logging.error(f"Failed to insert and commit OBS data to database with: {error}")
            logging.error(f"Failed frame: {decodedObsFrame}")
    return None


async def procRtcmStream(
    casterSettings: CasterSettings,
    mountPoint: str,
    dbSettings: DbSettings = None,
    fail: int = 0,
    retry: int = 3,
) -> None:
    # Initialize database connection and cursor to None
    dbConnection = None
    # dbCursor = None
    # Create instances of NtripStream and Rtcm3
    ntripstream = NtripStream()
    # rtcmMessage = Rtcm3()

    while True:
        try:
            # Connection handling
            while True:
                try:
                    await ntripstream.requestNtripStream(
                        casterSettings.casterUrl,
                        mountPoint,
                        casterSettings.user,
                        casterSettings.password,
                    )
                    break  # If the operation is successful, break the loop
                except (OSError):
                    fail += 1
                    sleepTime = 30
                    logging.error(f"Will retry NTRIP connection in {sleepTime} seconds!")
                    await asyncio.sleep(sleepTime)

            if dbSettings:
                while True:
                    try:
                        dbConnection = await dbConnect(dbSettings)
                        break
                        # If the operation is successful and fail is less than retry, break the loop
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
                        logging.error(f"Will retry database connection in {sleepTime} seconds!")
                        if dbConnection:
                            await dbConnection.close()
                        await asyncio.sleep(sleepTime)
                logging.info(f"Connected to database: {dbSettings.database}@{dbSettings.host}.")
            fail = 0
            encodedFrames = []
            decodedFrames = []
            storeObsCheck = dbSettings.storeObservations
            # print("Database connection established, starting to process RTCM stream.")
            while True:  # True
                try:
                    # timeStampAtGain = time()
                    rtcmFrame, timeStamp = await ntripstream.getRtcmFrame()
                    if (
                        rtcmFrame[24:-24].peek("uint:12") not in [1127, 1097, 1087, 1077]
                    ) and (rtcmFrame is not None):
                        continue
                    # print("RTCM frame received")
                    encodedFrames.append((rtcmFrame, timeStamp, len(rtcmFrame)))
                    # If there are more than 4 elements in the list, or the time since the last message exceeds 50ms
                    # runThroughTime = time()
                    if (
                        (
                            len(encodedFrames) >= 40
                            and all(
                                encodedFrames[-1][1] - encodedFrames[i][1] <= 0.05
                                for i in range(-2, -5, -1)
                            )
                        )
                        or (len(encodedFrames) > 80)
                        or (
                            (len(encodedFrames) > 20)
                            and (encodedFrames[-1][1] - encodedFrames[-2][1] >= 3.0)
                        )
                    ):
                        # Decode and insert the data
                        if not decodedFrames:
                            decodedFrames, decodedObs, tableList = await batchDecodeFrame(
                                encodedFrames, mountPoint, storeObsCheck)
                            # print("Decoded Frames")
                        encodedFrames.clear()
                        # print("Starting insertion")
                        await dbInsertBatch(
                            dbConnection, dbSettings, decodedFrames, decodedObs, tableList
                        )
                        # print("Insertion completed")
                        decodedFrames.clear()    
                except (ConnectionError, IOError):
                    fail += 1
                    sleepTime = 5 * fail
                    if sleepTime > 300:
                        sleepTime = 300
                    logging.error(
                        f"{mountPoint}: {fail} failed attempt to reconnect. "
                        f"Will retry in {sleepTime} seconds!"
                    )
                    await asyncio.sleep(sleepTime)
                    break
        except Exception as e:
            logging.error(f"An error occurred: {e}")
        finally:
            # Close the database connection and cursor if they were opened
            if dbConnection:
                await dbConnection.close()


async def rtcmStreamTasks(
    casterSettings: CasterSettings, dbSettings: DbSettings
) -> None:
    """
    This function creates and executes asyncio tasks for processing RTCM streams
    from multiple mountpoints.

    Parameters:
    casterSettings (CasterSettings): An instance of CasterSettings containing
                                     the caster URL, user credentials, and mountpoints.
    dbSettings (DbSettings): An instance of DbSettings containing database connection details.
    """

    # Initialize an empty dictionary to hold the tasks
    tasks = {}

    # For each mountpoint in the caster settings, create an asyncio task to process the RTCM stream
    for mountpoint in casterSettings.mountpoints:
        tasks[mountpoint] = asyncio.create_task(
            procRtcmStream(
                casterSettings,
                mountpoint,
                dbSettings=dbSettings,
            ),
            name=mountpoint,  # Name the task after the mountpoint for easy identification
        )

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
    ntripstream = NtripStream()

    # Initialize an empty list to hold the mountpoints
    mountpoints = []

    # Try to request the source table from the caster
    try:
        sourceTable = await ntripstream.requestSourcetable(casterSettings.casterUrl)
    # If a connection error occurs, log an error message, wait, and retry
    except ConnectionError:
        fail += 1
        logging.error(
            f"{fail} failed attempt to NTRIP connect to {casterSettings.casterUrl}. "
            "Will retry in {sleepTime} seconds."
        )
        asyncio.sleep(sleepTime)
    # If an unknown error occurs, log an error message and abort monitoring
    except Exception:
        logging.error("Unknown error. Abort monitoring.")
    # If the source table is successfully retrieved, extract the mountpoints
    else:
        for row in sourceTable:
            sourceCols = row.split(sep=";")
            # If the row represents a stream (STR), add the mountpoint to the list
            if sourceCols[0] == "STR":
                mountpoints.append(sourceCols[1])
        return mountpoints


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


async def profile_procRtcmStream(
    casterSettings: CasterSettings, dbSettings: DbSettings
):
    # Run the profiling function
    for mountpoint in casterSettings.mountpoints:
        pr = cProfile.Profile()
        pr.enable()
        await procRtcmStream(casterSettings, mountpoint, dbSettings=dbSettings)
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats("cumulative")
        ps.print_stats("ingest")  # Adjust this line to filter the results if needed
        print(s.getvalue())


def main(casterSettings: CasterSettings, dbSettings: DbSettings):
    """
    The main function that sets up signal handlers and starts the RTCM stream tasks.

    Parameters:
    casterSettings (CasterSettings): An instance of CasterSettings containing the caster settings.
    dbSettings (DbSettings): An instance of DbSettings containing the database connection details.
    """
    # Set up signal handlers for SIGINT, SIGTERM, and SIGALRM
    signal(SIGINT, procSigint)
    signal(SIGTERM, procSigterm)
    signal(SIGALRM, watchdogHandler)

    # Set up a timer that triggers the watchdog handler every 5 seconds
    setitimer(ITIMER_REAL, 0.2, 5)

    # Start the RTCM stream tasks
    print("Function is running on local github")
    asyncio.run(rtcmStreamTasks(casterSettings, dbSettings))
    # asyncio.run(profile_procRtcmStream(casterSettings, dbSettings))


# This code is only executed if the script is run directly
if __name__ == "__main__":
    # Declare global variables
    global casterSettings
    global dbSettings
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

    # Read configuration file
    config.read_file(open("/app/config/ingest.conf", mode="r"))
    # Set caster settings from config
    casterSettings.casterUrl = config["CasterSettings"]["casterUrl"]
    casterSettings.user = config["CasterSettings"]["user"]
    casterSettings.password = config["CasterSettings"]["password"]
    casterSettings.mountpoints = list(
        map(str.strip, config["CasterSettings"]["mountpoints"].split(","))
    )
    if casterSettings.mountpoints == [""]:
        casterSettings.mountpoints = []

    # Set database settings from config
    dbSettings.host = config["DbSettings"]["host"]
    dbSettings.port = config.getint("DbSettings", "port")
    dbSettings.database = config["DbSettings"]["database"]
    dbSettings.user = config["DbSettings"]["user"]
    dbSettings.password = config["DbSettings"]["password"]
    dbSettings.storeObservations = config.getboolean("DbSettings", "storeObservations")
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
    main(casterSettings, dbSettings)
