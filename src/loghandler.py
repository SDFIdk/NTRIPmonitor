import asyncio
import json
import logging
from time import gmtime, strftime, time

import asyncpg
from ntripclient import NtripClients
from settings import CasterSettings, DbSettings


class NtripLogHandler:
    def __init__(self):
        self.disconnect_id = None

    @staticmethod
    async def dbConnect(dbSettings: DbSettings):
        """
        Establishes a connection to the database using the provided settings.

        Parameters:
        dbSettings (DbSettings): An instance of DbSettings containing the database connection details.

        Returns:
        connection: A connection object that represents the database connection.
        """
        while True:
            try:
                connection = await asyncpg.connect(
                    user=dbSettings.user,
                    password=dbSettings.password,
                    host=dbSettings.host,
                    port=dbSettings.port,
                    database=dbSettings.database,
                )
                break
            except Exception as error:
                logging.error(f"Failed to connect to the database with: {error}")
                await asyncio.sleep(3)
        return connection

    async def requestStream(
        self,
        ntripclient: NtripClients,
        casterSettings: CasterSettings,
        dbSettings: DbSettings,
        mountPoint,
        log_disconnect: bool = True,
    ):
        """
        Initiates a connection to the NTRIP caster and requests the specified mount point.
        Also initiates a single database connection for logging disconnects and reconnects.
        Does not handle if the full process shuts down.

        Args:
        ntripclient (NtripClients): An instance of NtripClients to request the NTRIP stream.
        casterSettings (CasterSettings): An instance of CasterSettings containing the caster connection details.
        dbSettings (DbSettings): An instance of DbSettings containing the database connection details.
        mountPoint (str): The mount point to request from the NTRIP caster.
        log_disconnect (bool): Whether to log disconnects and reconnects. Default is True.
        log_disconnect is set to false during the initialization, as to not log non-true disconnects.
        """
        connection = await self.dbConnect(
            dbSettings
        )  # Change this to take from the connection pool from the database.
        while True:
            try:
                await ntripclient.requestNtripStream(
                    casterSettings.casterUrl,
                    mountPoint,
                    casterSettings.user,
                    casterSettings.password,
                )
                if log_disconnect and self.disconnect_id:
                    # If not initialization and mountpoint already disconnected
                    # update database entry with reconnect timestamp.
                    timestamp = time()
                    reconnect_time = strftime(
                        f"%Y-%m-%d %H:%M:%S.{int(timestamp % 1 * 1000000):06}",
                        gmtime(timestamp),
                    )
                    try:
                        await connection.execute(
                            """
                            SELECT update_reconnect_log($1::json)
                        """,
                            json.dumps([self.disconnect_id, reconnect_time]),
                        )
                    except asyncpg.exceptions.UndefinedFunctionError as e:
                        logging.error(
                            f"Could not update reconnect log. Stored procedure not found: {e}"
                        )
                    self.disconnect_id = None
                break
            except Exception as error:
                sleepTime = 5
                logging.error(
                    f"{mountPoint}: Will retry NTRIP connection in {sleepTime} seconds!"
                )
                if log_disconnect and not self.disconnect_id:
                    # If not initialization and mountpoint not already disconnected
                    # Insert database entry with disconnect timestamp.
                    timestamp = time()
                    disconnect_time = strftime(
                        f"%Y-%m-%d %H:%M:%S.{int(timestamp % 1 * 1000000):06}",
                        gmtime(timestamp),
                    )
                    logging.info(f"{mountPoint}: Disconnected at {disconnect_time}")
                    try:
                        result = await connection.fetchval(
                            """
                            SELECT insert_disconnect_log($1::json)
                        """,
                            json.dumps([mountPoint, disconnect_time]),
                        )
                        self.disconnect_id = result
                    except asyncpg.exceptions.UndefinedFunctionError as e:
                        logging.error(
                            f"Could not update disconnect log. Stored procedure not found: {e}"
                        )
                await asyncio.sleep(sleepTime)

        await connection.close()
        return ntripclient
