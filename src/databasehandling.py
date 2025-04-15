import asyncio
import asyncpg
import json
import logging
from time import sleep, gmtime, strftime, time

from settings import CasterSettings, DbSettings, MultiprocessingSettings
from ntripclient import NtripClients


class DatabaseHandler:
    def __init__(self, dbSettings):
        self.dbSettings = dbSettings
        self.pool = None

    async def initializePool(self):
        while True:
            try:
                self.pool = await asyncpg.create_pool(
                    user=self.dbSettings.user,
                    password=self.dbSettings.password,
                    host=self.dbSettings.host,
                    port=self.dbSettings.port,
                    database=self.dbSettings.database,
                    min_size=1,  # Minimum number of connections in the pool
                    max_size=20,  # Maximum number of connections in the pool
                )
                break
            except Exception as error:
                logging.error(f"Failed to create connection pool with: {error}")

    async def closePool(self):
        if self.pool:
            await self.pool.close()

    async def getConnection(self):
        if not self.pool:
            raise Exception("Connection pool is not initialized.")
        return await self.pool.acquire()

    async def releaseConnection(self, connection):
        if self.pool and connection:
            await self.pool.release(connection)

    @staticmethod
    async def waitDbConnection(dbSettings):
        while True:
            try:
                dbConnection = await DatabaseHandler.dbConnect(dbSettings)
                await dbConnection.close()
                sleep(1)
                break
            except Exception as error:
                logging.info("Database connection is not yet open. Waiting...")
                sleep(3)
        logging.info("Database is initialized. Initializing the monitor system.")


class NtripObservationHandler(DatabaseHandler):
    INPUTTABLE = {
        1001: "insert_gps_observations",
        1002: "insert_gps_observations",
        1003: "insert_gps_observations",
        1004: "insert_gps_observations",

        1005: "upsert_coordinates",
        1006: "upsert_coordinates",

        1009: "insert_glonass_observations",
        1010: "insert_glonass_observations",
        1011: "insert_glonass_observations",
        1012: "insert_glonass_observations",

        1071: "insert_gps_observations",
        1072: "insert_gps_observations",
        1073: "insert_gps_observations",
        1074: "insert_gps_observations",
        1075: "insert_gps_observations",
        1076: "insert_gps_observations",
        1077: "insert_gps_observations",

        1081: "insert_glonass_observations",
        1082: "insert_glonass_observations",
        1083: "insert_glonass_observations",
        1084: "insert_glonass_observations",
        1085: "insert_glonass_observations",
        1086: "insert_glonass_observations",
        1087: "insert_glonass_observations",

        1091: "insert_galileo_observations",
        1092: "insert_galileo_observations",
        1093: "insert_galileo_observations",
        1094: "insert_galileo_observations",
        1095: "insert_galileo_observations",
        1096: "insert_galileo_observations",
        1097: "insert_galileo_observations",

        1101: "insert_sbas_observations",
        1102: "insert_sbas_observations",
        1103: "insert_sbas_observations",
        1104: "insert_sbas_observations",
        1105: "insert_sbas_observations",
        1106: "insert_sbas_observations",
        1107: "insert_sbas_observations",

        1111: "insert_qzss_observations",
        1112: "insert_qzss_observations",
        1113: "insert_qzss_observations",
        1114: "insert_qzss_observations",
        1115: "insert_qzss_observations",
        1116: "insert_qzss_observations",
        1117: "insert_qzss_observations",

        1121: "insert_beidou_observations",
        1122: "insert_beidou_observations",
        1123: "insert_beidou_observations",
        1124: "insert_beidou_observations",
        1125: "insert_beidou_observations",
        1126: "insert_beidou_observations",
        1127: "insert_beidou_observations",

        1121: "insert_beidou_observations",
        1122: "insert_beidou_observations",
        1123: "insert_beidou_observations",
        1124: "insert_beidou_observations",
        1125: "insert_beidou_observations",
        1126: "insert_beidou_observations",
        1127: "insert_beidou_observations",
    }

    def __init__(self, dbSettings):
        super().__init__(dbSettings)

    async def grabStoredProcedure(msgType: int):
        return NtripObservationHandler.INPUTTABLE.get(msgType, None)

    async def dbInsertObsInfoStoredBatch(
        self, decodedFrames: list, decodedObs: list, rtcmPackageIds: list
    ) -> list:
        for index, decodedObsFrame in enumerate(decodedObs):
            if decodedObsFrame is None:
                continue
            rtcmId = rtcmPackageIds[index]
            try:
                decodedObsFrame = [(rtcmId, *obsFrame) for obsFrame in decodedObsFrame]
            except Exception as error:
                logging.error(
                    f"Error frame in iterable: {error} with frame {decodedObsFrame}"
                )
            try:
                decodedObsFrameJson = json.dumps(decodedObsFrame)
                connection = await self.getConnection()
                try:
                    stored_procedure = await NtripObservationHandler.grabStoredProcedure(
                        decodedFrames[index][3]
                    )
                    if stored_procedure is None:
                        logging.error(
                            f"No stored procedure found for RTCM message identifier: {decodedFrames[index][3]}"
                        )
                        continue
                    query = f"SELECT {stored_procedure}($1::json)"
                    logging.debug(f"Executing query: {query}")
                    await connection.execute(query, decodedObsFrameJson)
                finally:
                    await self.releaseConnection(connection)
            except Exception as error:
                logging.error(f"Database handling of frame: {decodedObsFrame}")
                logging.error(f"rtcmId: {rtcmId}")
                logging.error(
                    f"Failed to insert and commit observational data to database with: {error}"
                )
        return None

    async def dbInsertRtcmInfoStoredBatch(self, decodedFrames: list) -> list:
        try:
            decodedFramesJson = json.dumps(decodedFrames)
            connection = await self.getConnection()
            try:
                rtcmPackageIds = await connection.fetchval(
                    "SELECT insert_rtcm_packages($1::json)", decodedFramesJson
                )
            finally:
                await self.releaseConnection(connection)
        except Exception as error:
            logging.error(
                f"Failed to insert and commit RTCM data to database with: {error}."
            )
            rtcmPackageIds = None
        return rtcmPackageIds

    async def dbInsertBatch(
        self,
        decodedFrames: list,
        decodedObs: list,
    ) -> None:
        try:
            rtcmPackageIds = await self.dbInsertRtcmInfoStoredBatch(decodedFrames)
            if self.dbSettings.storeObservations and rtcmPackageIds is not None:
                await self.dbInsertObsInfoStoredBatch(
                    decodedFrames, decodedObs, rtcmPackageIds
                )
            elif rtcmPackageIds is not None:
                logging.debug(
                    "RTCM package ID returned as None. Not storing observations."
                )
        except Exception as error:
            logging.error(
                f"Failed to insert and commit data to database with: {error}."
            )
        return None


class NtripLogHandler(DatabaseHandler):
    def __init__(self, dbSettings: DbSettings, mountpoint: str):
        super().__init__(dbSettings)
        self.mountpoint = mountpoint
        self.disconnect_id = None

    async def insert_disconnect_log(self):
        timestamp = time()
        disconnect_time = strftime(
            f"%Y-%m-%d %H:%M:%S.{int(timestamp % 1 * 1000000):06}",
            gmtime(timestamp),
        )

        try:
            connection = await self.getConnection()
            result = await connection.fetchval(
                """
                SELECT insert_disconnect_log($1::json)
            """,
                json.dumps([self.mountpoint, disconnect_time]),
            )
            self.disconnect_id = result
            await self.releaseConnection(connection)
        except asyncpg.exceptions.UndefinedFunctionError as e:
            logging.error(
                f"Could not update disconnect log. Stored procedure not found: {e}."
            )

    async def update_reconnect_log(self):
        timestamp = time()
        reconnect_time = strftime(
            f"%Y-%m-%d %H:%M:%S.{int(timestamp % 1 * 1000000):06}",
            gmtime(timestamp),
        )
        try:
            connection = await self.getConnection()
            await connection.execute(
                """
                SELECT update_reconnect_log($1::json)
            """,
                json.dumps([self.disconnect_id, reconnect_time]),
            )
            self.disconnect_id = None
            await self.releaseConnection(connection)
        except asyncpg.exceptions.UndefinedFunctionError as e:
            logging.error(
                f"Could not update reconnect log. Stored procedure not found: {e}"
            )

    async def requestStream(
        self,
        ntripclient: NtripClients,
        casterSettings: CasterSettings,
        log_disconnect: bool = True,
    ):
        """
        Initiates a connection to the NTRIP caster and requests the specified mount point.
        Also initiates a single database connection for logging disconnects and reconnects.
        Does not handle if the full process shuts down. This is handled by RunMultiProcessing in ingestion.py.

        Args:
        ntripclient (NtripClients): An instance of NtripClients to request the NTRIP stream.
        casterSettings (CasterSettings): An instance of CasterSettings containing the caster connection details.
        log_disconnect (bool): Whether to log disconnects and reconnects. Default is True.
        log_disconnect is set to false during the initialization as to not log disconnects.
        """

        if log_disconnect and not self.disconnect_id:
            # If not initialization and mountpoint not already disconnected
            # Insert database entry with disconnect timestamp.
            await self.insert_disconnect_log()
            logging.info(f"{self.mountpoint}: Connection lost.")

        while True:
            try:
                await ntripclient.requestNtripStream(
                    casterSettings.casterUrl,
                    self.mountpoint,
                    casterSettings.user,
                    casterSettings.password,
                )
                if log_disconnect and self.disconnect_id:
                    # If not initialization and mountpoint already disconnected
                    # update database entry with reconnect timestamp.
                    await self.update_reconnect_log()
                    logging.info(f"{self.mountpoint}: Connection reestablished.")
                break
            except Exception as error:
                sleepTime = 5
                logging.error(
                    f"{self.mountpoint}: Will retry NTRIP connection in {sleepTime} seconds!"
                )

                await asyncio.sleep(sleepTime)
        return ntripclient
