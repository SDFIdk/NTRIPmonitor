# Development and To-Do's
## Known Issues
### .env loading
If you wish to create an environment file for both the Production or Development, you must consider the presedence of .env files. If the .env files for both the production and development environment are in the same directory, they will load from eachother and take presedence. Thus, in case you name one environment file "development.env", while another environment file ".env" is located in the same directory, the .env will overwrite the development.env variables. It does not matter whether you've stated it directly in the docker-compose, as they are located in the same folder as the Dockerfile and the Docker-compose. Split the environment files into their own folders and edit the Dockerfile and Docker-compose to load the correct environment file.
## Development
### Daily / hourly statistic files
In the current framework, this can be easily implemented. The disconnect / reconnect entries are already present in database in table [initdb/dev/30-connection_logger.sql](30-connection_logger.sql). To compute daily statistic files, pull database entries and compute the relevant statistics into either a pdf or a log / txt file. Grafana can be set to continously check the Connection_logger table for new entries and push a notification to the user whenever.

### NMEA pings for VRS test
A base-code for sending NMEA pings can be found in the [src-dev/virtualreferencestation.py](virtualreferencestation.py) class. The current concept was intended to function by having a shapefile .shp file containing a polygon of the region in which you wish to test your VRS network, and a low spatial resolution geoid / DEM (5x5 / 10x10) with coordinates. It automatically calculates the message checksum and appends to the NMEA sentence.

The random-test is intended to work by:
- Assuming a uniform distribution in test points
- Creating a coordinate point within the polygon boundary
- Finding the related altitude to the coordinate point from the low resolution geoid / DEM
- Constructing the relevant NMEA sentence (GGA, RMC, GSA, GSV, VTG, GLL)
- Pinging the network with the user-preferred protocol (HTTP / TCP / UDP)
- Storing the result in a database table for comparison and analysis

The test can be modified to be performed on fixed coordinates as well.

There is a number of different ways to store a low or high resolution DEM and using it efficiently for gathering the altitude. For a low resolution DEM / Geoid, it can simply be stored in the memory. In cases where a high-resolution DEM / geoid is a hard requirement for testing purposes, the Geoid can be split into a series of files, applying a name system based on the bounding box of each file, and therefore only load in the relevant part of the DEM based on the random coordinate.

Furthermore, the testing can be done non-random, and instead be run over a pre-defined series of points to compare return tests.

### Direct IP connection to mountpoints
Currently not implemented. The connection procedure is essentially the same as the current functionality for connecting to a caster, and can therefore easily be implemented. For direct IP connecting, I propose creating a logfile containing columns with each direct connection
"MountpointName, IP, ..." 
and loading it upon initialization of the mountpoint software. This reduces potential clutter in the .env file, and ensures that handling of direct IP connections are done seperately. 

### Change rtcm reading procedure to be continous instead of on a per-frame-basis
Currently, the procedure for reading binary RTCM frames are done on a per-frame-basis. The function getRtcmFrame in the [src-dev/ntripclient.py](ntripclient.py) class is called and reads the datastream until a complete frame is received before returning it. This should be changed, such that the reading process is not on a per-frame-basis, but rather a continous reading. 

### Optimize mountpointsplitter 
The mountpoint splitter is in its current format decent, but should be optimized for multiple casters and estimated message latency to reduce 
