import asyncpg
import json 
import logging
from time import sleep

from settings import CasterSettings, DbSettings, MultiprocessingSettings
class DatabaseHandler:
    INPUTTABLE = {
        "gps": "insert_gps_observations",
        "coordinates": "upsert_coordinates",
        "glonass": "insert_glonass_observations",
        "galileo": "insert_galileo_observations",
        "sbas": "insert_sbas_observations",
        "qzss": "insert_qzss_observations",
        "beidou": "insert_beidou_observations",
    }

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
                    max_size=20  # Maximum number of connections in the pool
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

    async def grabStoredProcedure(inputString: str):
        return DatabaseHandler.INPUTTABLE.get(inputString, None)

    async def dbInsertObsInfoStoredBatch(self, decodedObs: list, rtcmPackageIds: list, tableList: list) -> list:
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
                connection = await self.getConnection()
                try:
                    stored_procedure = await DatabaseHandler.grabStoredProcedure(tableList[index])
                    if stored_procedure is None:
                        logging.error(f"No stored procedure found for table identifier: {tableList[index]}")
                        continue
                    query = f"SELECT {stored_procedure}($1::json)"
                    logging.debug(f"Executing query: {query}")
                    await connection.execute(query, decodedObsFrameJson)
                finally:
                    await self.releaseConnection(connection)
            except Exception as error:
                logging.error(f"databasehandling : {decodedObsFrame}")
                logging.error(f"rtcmid : {rtcmId}")
                logging.error(f"Failed to insert and commit observational data to database with: {error}")
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
            logging.error(f"Failed to insert and commit RTCM data to database with: {error}")
            rtcmPackageIds = None
        return rtcmPackageIds

    async def dbInsertBatch(self, dbSettings: DbSettings, decodedFrames: list, decodedObs: list, tableList: list) -> None:
        try:
            rtcmPackageIds = await self.dbInsertRtcmInfoStoredBatch(decodedFrames)
            if dbSettings.storeObservations and rtcmPackageIds is not None:
                await self.dbInsertObsInfoStoredBatch(decodedObs, rtcmPackageIds, tableList)
            elif rtcmPackageIds is not None:
                logging.debug("Rtcmpackage ID are returned as None. Not storing observations.")
        except Exception as error:
            logging.error(f"Failed to insert and commit data to database with: {error}")
        return None
