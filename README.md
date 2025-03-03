# Table of Contents
- [Quick-start](#quick-start)
  - [Short overview of the solution](#short-overview-of-the-solution)
  - [Run requirements and how-to](#run-requirements-and-how-to-build)
    - [Environment variables](#environment-variables)
    - [System requirements and docker build information](#system-requirements-and-docker-build-information)
    - [Python library requirements](#python-library-requirements)
    - [Configuring the .env](#configuring-the-env)
    - [Building docker images and deploying a docker network](#building-docker-images-and-deploying-a-docker-network)
    - [New User Check List](#new-user-check-list)
- [Ingestion](#ingestion)
  - [Multiprocessing](#multi-processing)
    - [Overview of multiprocessing environment variables](#overview-of-multiprocessing-variables)
    - [Multiprocessing - Reading processes](#multiprocessing---reading-processes)
      - [Process of the individual reading process](#process-of-the-individual-reading-process)
    - [Multiprocessing - Decoder processes](#multiprocessing---decoder-processes)
      - [process of the individual decoder process](#process-of-the-individual-decoder-process)
  - [Single processing](#single-processing)
    - [Asynchronous operations](#asynchronous-operations)
  - [Disconnect/Reconnect log handling](#disconnectreconnect-log-handling)
  - [NTRIP, RTCM3 and Decoding](#ntrip-rtcm3-and-decoding)
    - [NtripClient Module](#ntripclient-module)
    - [RTCM3 Module](#rtcm3-module)
    - [Decoder Module](#decoder-module)
- [Database](#database)
  - [Stored procedures](#stored-procedures)
  - [Database connections](#database-connections)
- [Grafana](#grafana)

# Quick-start

## Short overview of the solution
The NTRIPmonitor is a monitoring application designed to ingest Real-Time Kinematic (RTK) correction data in the RTCM format from an NTRIP caster, store and contain the data in the UREGA postgresql database and visualize it using the grafana web application.

The monitor system consists of three subsystems: 
- The ingestion script written in Python
- The PostgreSQL database with the TimescaleDB extension
- The Grafana web application

The ingestion application architecture allows for concurrent handling of multiple NTRIP streams through use of multiprocessing.

The multiprocessing model is implemented through reader processes that connect to the NTRIP caster and read the RTCM data from multiple sets of mountpoints. Within each reader process, the streams from individual mountpoints are read asynchronously to reduce the required computation power albeit with a slight increase in processing time.
The reader processes store the data in shared memory. Seperate decoder processes read from the shared memory, decode the data and insert it into the UREGA database using stored procedures.

The NTRIPmonitor uses the asyncpg library for efficient handling of PostgreSQL database interactions. For any multiprocessing operations, the monitor solution uses Python's built in multiprocessing library, and the asyncio library for asynchronous operations to handle IO bound tasks in each process efficiently.
## Run requirements and how to build
In this section we briefly explain requirements for launching the monitor solution. Read this section carefully and follow the [Setup Check List](#Setup-Check-List).
### Environment variables
The monitor solution uses a locally saved environment file. The environment file is required for setting the processing settings and used to declare caster/database/grafana credentials. An example of an environment file can be found at [.env.example](.env.example). Copy the example and name it ".env". Modify the settings within this file according the use case. The .env file must be located in the same folder as the [Dockerfile](Dockerfile) and [docker-compose.prod.yaml](docker-compose.prod.yaml)/[docker-compose.dev.yaml](docker-compose.dev.yaml).
### System requirements and docker build information
The monitor solution is built as a series of docker containers in order to be easily deployable on any system. For information regarding the current container setup, we refer to the [docker-compose.dev.yaml](docker-compose-dev) for the test setup and [docker-compose.prod.yaml](docker-compose-prod) for the production deployment. The current setup deploys 3 containers in the monitor network; Ingestion, Grafana, Timescaledb. The grafana and timescaledb containers are pulled from their respective docker registry, whereas the Ingestion container must be built using the Dockerfile for the [Dockerfile]. 
### Python library requirements
The current monitor solution requires three well-known packages to function.
- **bitstring** is a python module that provides classes to create, manipulate and interpret binary data, allowing for easy handling of binary data structures, including reading and writing bits and bytes.
- **asyncpg** is an efficient fully asynchronous PostgreSQL client library for Python. It is designed for scalability and high performance with integration of modern PostgreSQL features.
- **python-dotenv** is a Python library that reads key-value pairs from an environment file (.env), and can set them as environment variables. This is useful for managing configuration settings and sensitive information in a simple and secure way.

### Configuring the .env

Configuration of the NTRIPmonitor is done by creating a configuration file called [.env] containing values for a set of environment variables, which are loaded by docker-compose.
This repository contains an example [.env.example] that can be used as a template.
Save the template as a new file [.env] and fill the required information:

The first section of variables deal with local resources:
| Variable | Description |
|---|---|
| NTRIP_DOCKER_REGISTRY | Location of NTRIPmonitor docker image. If building locally set to ntripmonitor:latest |
| NTRIP_DATAMOUNT | Location of data to store. If set to Empty, there wil be no data persistence. Only used in production environment.:q |
| DB_CPU | Integer number of CPU's available for the database. |
| DB_MEMORY | Amount of memory availabe for the database. E.g. 32GB |
| INGEST_CPU | Integer (FLOAT?) number of CPU's available for ingestion. E.g. 8 |
| INGEST_MEMORY | Amount of memory availabe for the ingestion. E.g. 8GB |

The second section of variables deal with the database setup. All can be left to their default values.
| Variable | Description |
|---|---|
| DB_USER | Choose a username for the database. Default: postgres |
| DB_PASSWORD | Choose a password for the database. |
| DB_NAME | Default: UREGA |
| DB_HOST | Default: timescaledb |
| DB_PORT | Port for accessing the database. Default: 5432 |
| DB_STORE_OBSERVATIONS | Storing observations extracted from MSM messages can lead to large amounts of data. Default: True |

The third section of variables deal with Grafana
| Variable | Description |
|---|---|
| GRAFANA_USER | Choose a username for Grafana. |
| GRAFANA_PASSWORD | Choose a password for Grafana. |

The fourth section of variables are the multiprocessing settings. These are described in the section on Multiprocessing below.

The remaining sections configure one or more casters. Different casters can be separated by using different characters before the first underscore.
| Variable | Description |
|---|---|
| FIRST_CASTER_ID | Name for caster to be used internally |
| FIRST_CASTER_URL | URL and port of caster. E.g.: https://ntrip.dataforsyningen.dk:443 |
| FIRST_CASTER_USER | Username to log on to caster |
| FIRST_CASTER_PASSWORD | Password |
| FIRST_CASTER_MOUNTPOINT | Comma-separated list of mountpoints to monitor. E.g. BUDD,MOJN |

> [!WARNING]
> Be careful not to save sensitive information in the template to avoid accidentally uploading this to Github.

### Building docker images and deploying a docker network

In a terminal, enter the directory of this code. To build the docker image, execute the following command:

```
docker buildx build -t ntripmonitor:latest .
```
This will build the docker image and store it locally under the name "ntripmonitor:latest".

Make sure you have configured the file [.env] before deploying the network.
To deploy the test environment.

```
docker-compose -f docker-compose.dev.yaml up -d
```

The flag `-d` starts the docker environment in the background.
If you are ready to deploy the production environment replace `docker-compose.dev.yaml` with `docker-compose.prod.yaml`.

Your NTRIPmonitor instance is now up and running.

### Setup Check List
Checklist for launching the monitor solution. 
- Download the source code for the application from GitHub.
- Extract source code to a directory of your choosing.
- Copy the .env.example, and rename the copy ".env".
- Configure the .env file to fit your setup.
- Build the ingestion image using the Dockerfile.
- Deploy the NTRIPmonitor network with docker-compose.

# Ingestion
The ingestion of the monitor solution is responsible for all functionality regarding reading, decoding, and general handling of the datastreams. 
## Multiprocessing
To enable the multiprocessing version of the monitor solution, set the variable `MULTIPROCESSING_ACTIVE` to True in the environment file. This enables the use of multiple cores.
The number of cores the system will use is set in the environment file.
### Overview of multiprocessing variables
- **MULTIPROCESSEING_ACTIVE**: BOOL: Whether to use multiprocessing or not.
- **MAX_READERS** - INTEGER: Maximum amount of reader processes to spawn. Each reader process is granted a CPU core, if adequate resources are available.
- **READERS_PER_DECODER** - INTEGER: How many reader processes should share a decoder process. Each decoder is granted a CPU core, if adequate resources are available.
- **CLEAR_CHECK** - FLOAT: The time interval at which each decoder process will check the shared memory for new data entries.
- **APPEND_CHECK** - FLOAT: The time interval each reader process will append to the shared memory when receiving data.
- **INGEST_CPU** - FLOAT: Number of CPU the application has access to. The application will not block other applications from using the allocated CPU's, but the application may, if required at times, use this amount of CPU's.
- **INGEST_MEMORY** - INTEGER + GB : The amount of memory the application has access to. Similar to the CPU, the application will not block other applications from using the allocated memory, but the application may, if required at times, use this amount of memory.
### Multiprocessing - Reading processes
For the multiprocessing function tree, we advise the reader to follow the function calls of the function `RunRTCMStreamTasks` in [src/ingestion.py](src/ingestion.py).
The maximum number of reading processes spawned is controlled by the **MAX_READERS** multiprocessing environment variable, but the true resulting reading processes also depends on the amount of configured mountpoints. The mountpoints are split into equal sized chunks, as to strain each reader process equally. If the amount of mountpoints are less than the maximum allowed reader processes, a reader process will be dedicated to each mountpoint and the remainder reading processes will be killed.
#### Process of the individual reading process
Each reading process is fed a mountpoint chunk. As each reading process is spawned, a `databaseHandler` class with a connection pool to the database is created. Each reading process creates an asynchronous operation for each mountpoint in the mountpoint chunk. A watchdog process watching over the asynchronous operations is spawned in order to reboot any shut down asynchronous operations during run time. In each operation, instances of the classes `Ntripclient` and `RTCM3` class is created. In each asynchronous operation we listen and log all messages sent by the mountpoint, and append to the shared memory. A continuous check is run for each mountpoint on their respective datastream every 0.05 seconds. If the datastream has gone silent, e.g. no incoming messages for 0.05 seconds, the process acquires the CPU lock and appends the messages to the shared memory, for processing by the assigned decoder process.
Each decoder process handles disconnects and reconnects to either the database or the ntripcaster.
Reading process in simple terms:
- Reading process is spawned with a list of mountpoints
- For each mountpoint in the list, an asynchronous operation is spawned.
- - Each asynchronous operation connects to the mountpoint datastream
- - Continuously tries to connect to both the database and ntripcaster until both connections are established.
- - Grabs all messages sent by the mountpoint per second
- - After the datastream has been idle for 0.05 seconds, grabs the CPU lock and appends message list to shared memory
- - Repeat

### Multiprocessing - Decoder processes
The maximum number of decoder processes depend upon the number of reading processes spawned and the `READERS_PER_DECODER` multiprocessing environment variable. For each decoder process a CPU lock and a shared memory manager are spawned, which are fed to the reading processes related to that decoder process.
#### Process of the individual decoder process
Each decoder process is fed a CPU lock and a shared memory manager. Each reading process is spawned with a `databaseHandler` class and a `Decoder` class to handle the incoming RTCM messages.

The decoder process in simple terms: 
- The decoder process is spawned with a CPU lock and a shared memory manager.
- The decoder process continuously checks the shared memory each 0.05 seconds for new data entries. 
- If data is present in the shared memory, it grabs the CPU lock and clears the shared memory for data entries.
- Each dataentry is run through the `Decoder` class, which checks the message type and assigns the correct decoder to the message type. 
- Each dataentry is decoded and assigned a table string.
- The data is packed in a JSON format and sent to the database through a stored procedure. The stored procedures to use is determined by the table string. 
- Repeat

## Single-core processing

Functionality under development.
Documentation coming soon.

### Asynchronous operations

Documentation coming soon.

## Disconnect/Reconnect log handling
Disconnects and reconnects of individual mountpoints are currently handled in the [src/databasehandling.py](NtripLogHandler) class.
Mountpoint disconnects are logged and timestamps are inserted into the database table [initdb/30-connection_logger.sql](initdb/30-connection_logger.sql).
The dataentry is updated with a timestamp when a new connection to the mountpoint has been established.

## NTRIP, RTCM3 and Decoding
### NtripClient Module
The `NtripClients` class in [src/ntripclient.py](src/ntripclient.py) is a key component for managing NTRIP connections in the monitor solution. It handles establishing connections with NTRIP servers, sending requests, and processing RTCM data streams.
The `NtripClients` class handles the following:

- Connect to NTRIP servers: Open connections to both HTTP and HTTPS NTRIP servers.
- Send requests: Create and send various types of HTTP requests to interact with the server, including source tables and data streams.
- Receive and Process Data: Handle incoming RTCM data, manage chunked or non-chunked streams, and ensure data integrity through qualcomm checksum.
[insert flowchart, ppx slide #2]
### RTCM3 Module
The `RTCM3` class at [src/rtcm3.py](src/rtcm.py) is the primary typesetting module for transforming the binary data into a more readable format. It is technically responsible for handling and prasing all RTCM v3 messages coming in from the datastreams. Inside the `RTCM3` class, we store all information related to incoming binary RTCM messages, which includes the structure of the message and typesets for both header messages and observation messages. The class has further been developed to include functionality for decoding and encoding binary frames:
- Storing information about the binary RTCM messages, including their typesets.
- Handling all header messages and observation messages.
- Encoding and decoding RTCM data and headers.
[insert flowchart, ppx slide #3]
### Decoder Module
The `Decoder` class in [src/decoderclasses.py](src/decoderclasses.py) is where the post-message-retrieval decoding occurs. If the received data needs to be processed further, or only snippets of the received dataframe is required, they are handled further in the `Decoder` class. The `Decoder` class is responsible for decoding the incoming message types according to our prefered use case. The decoder classes are created to have a dictionary as input and output, in order to be easily scaleable for future messagetype additions. Each decoded message is assigned a "table", which is simply a string within the decoder function, which tells the database insertion function which stored procedure it should apply to the message. The current decoding functionality consists of
- Decoding antenna coordinate (1005/1006)
- All MSM message types. (1071-1137)
[insert flowchart, ppx slide #4]

# Grafana
[Grafana](https://grafana.com/docs/grafana/latest/) is used to visualize real-time GNSS data by creating dynamic and interactive dashboards. For the monitor solution we use Grafana version 9.5.19. We use PostgreSQL queries within Grafana to fetch and display data from the database. By utilizing Grafana macros for PostgreSQL, we can efficiently query and visualize the GNSS data. For For more details on the panel types and macros available for PostgreSQL, we refer to the [Grafana Panel Documentation](https://grafana.com/docs/grafana/latest/panels-visualizations/visualizations/) and the [Grafana PostgreSQL documentation](https://grafana.com/docs/grafana/latest/datasources/postgres/).

The grafana container loads the pre-configured dashboards found at [initgrafana/dashboards/](initgrafana/dashboards/) and stores both the [Inspector Gadget](initgrafana/dashboards/Inspector%20Gadget.json) and [RTCM overview](initgrafana/dashboards/RTCM%20monitor.json) dashboards. We highly recommend to use the interactive dashboard builder on the grafana webpage to create the dashboards, and update your dash boards in your local configuration. Each visualization can be built by using Grafanas interactive PostgreSQL query builder or by writing queries directly in plain SQL. 
> [!NOTE]
> Saving your dashboard on the grafana webpage **does NOT** save it persistently. You must copy the changed dashboard code by going into settings -> JSON code in the respective dashboard, and copy it over to your local file.
# Database
The database uses the [timescaledb](https://docs.timescale.com/) extension to [PostgreSQL](https://www.postgresql.org/docs/). We use the latest postgreSQL 14 version.
## Stored procedures
Database interactions between the ingestion and timescaledb containers are executed using stored procedures. During ingestion the data is packed in JSON format and sent to a stored procedure in the database.
The stored procedure handles the sorting and type setting of the packed data in the database.
Stored procedures reduce CPU usage on the ingestion container by reducing the database interaction to a transmission of a data pack, and therefore solely IO bound.
Stored procedures are functions existing solely in the database, and are responsible for both type setting and sorting the ingestion data into the correct tables and columns.

The received data pack contains a series of data entries to minimize the number of interactions, and therefore the connection overhead, between the ingestion and database container.
The number of data entries sent in each pack depends on the data type and insertion frequency set by the user.
## Database connections
Whether running the monitor solution by single processing or multi processing, the monitor solution utilizes a pool of database connections.
Using a pool of connections minimizes the overhead when creating new connections, and improves interaction performance compared to having only one continuously open connection. 
