#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod
import logging
from datetime import datetime
from math import pow
from time import gmtime, strftime, time

from rtcm3 import Rtcm3

class Decoder(ABC):
    def __init__(self,params):
        self.params = params
        self.data = params.get('data',None)

    @staticmethod
    def gnssEpochStr(messageType: int, obsEpoch: float, Type: int) -> str:
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


    @staticmethod
    async def batchDecodeFrame(encodedFrames: list, storeObsCheck, rtcmMessage: Rtcm3):
        decodedFrames = []
        decodedObs = []
        tableList = []
        for params in encodedFrames:
            try:
                decodedFrame, decoderResult = Decoder.getDecoder(params, storeObsCheck, rtcmMessage)
                decodedFrames.append(decodedFrame)
                if decoderResult is not None:
                    decodedObs.append(decoderResult["decodedObs"])
                    tableList.append(decoderResult["table"])
                else:
                    decodedObs.append(None)
                    tableList.append(None)
            except Exception as error:
                logging.error(f"Errors in grabbing decoder class : {error}")
                continue
        return decodedFrames, decodedObs, tableList

    @staticmethod
    def getDecoder(params, storeObsCheck, rtcmMessage: Rtcm3):
        try: 
            params['messageType'], params['data'] = rtcmMessage.decodeRtcmFrame(params['frame'])
        except Exception as error:
            logging.error(f"Failed to decode RTCM frame with error: {error}")
            return None, None
        decodedFrame = Decoder.rtcmSimpleMetadata(params)
        decoderClass = None
        decodedObs = None
        # logging.info(params['messageType'])
        if storeObsCheck:
            decoderClass = DECODER_MAP.get(params['messageType'])
            if decoderClass is None:
                logging.debug(f"Message type {params['messageType']} not supported")
                return decodedFrame, None
            else:
                logging.debug(f"Message type {params['messageType']} supported")
                decoderInstance = decoderClass(params)
                try:
                    decodedObs = decoderInstance.decode()
                    if decodedObs is None or not decodedObs:
                        return decodedFrame, None
                except Exception as error:
                    logging.error(f"Failed to decode RTCM OBS frame with error: {error}")
                    return decodedFrame, None
        return decodedFrame, decodedObs

    @staticmethod
    def rtcmSimpleMetadata(params: dict):
        try:
            if params['messageType'] >= 1071 and params['messageType'] <= 1127:
                satCount = len(params['data'][1])
                obsEpochStr = Decoder.gnssEpochStr(params['messageType'], params['data'][0][2] / 1000.0, 1)
            else:
                satCount = None
                obsEpochStr = None
            decodedFrame = (
                params['mountPoint'],
                strftime(
                    f"%Y-%m-%d %H:%M:%S.{int(params['timeStampInFrame'] % 1 * 1000000):06}",
                    gmtime(params['timeStampInFrame']),
                ),
                obsEpochStr,
                params['messageType'],
                params['messageSize'],
                satCount,
            )
        except Exception as error:
            logging.info(f"Failed to decode simple metadata {params['messageType']}: {error}")
        return decodedFrame
    

    @abstractmethod
    def decode(self):
        pass

class DecoderTypeXXX(Decoder):
    def decode(self):
        pass

class DecoderPOS(Decoder):
    def __init__(self, params):
        super().__init__(params)
        self.data = params['data']
        self.mountPoint = params['mountPoint']
        self.messageType = params['messageType']
        self.decodedObs = []
        self.table = []

    def decode(self):
        try:
            try:
                _, self.table = SAT_TYPE_TABLE_DICT.get(self.messageType)
                if self.data is None and not self.data:
                    return {"decodedObs": [], "table": self.table}
            except Exception as error:
                logging.error(f"Failed to get SAT_TYPE_TABLE_DICT {error}")
            
            # ARP given in mm as integer, convert to m as float
            x = self.data[1][0] / 10000.0
            y = self.data[1][1] / 10000.0
            z = self.data[1][2] / 10000.0
            # Antenna reference height not included in message type 1005
            antHgt = None
            if self.messageType == 1006:
                antHgt = self.data[1][3] / 10000.0
            
            self.decodedObs.append([self.messageType,
                               self.mountPoint,
                               x,
                               y,
                               z,
                               antHgt])
        except Exception as error:
            logging.error(f"Failed to decode ARP message with error: {error}. Setting observation to None")
            self.decodedObs = None
        
        return {"decodedObs": self.decodedObs, "table": self.table}
        

class DecoderMSM(Decoder):
    def __init__(self, params):
        super().__init__(params)
        self.data = params['data']
        self.timeStampInFrame = params['timeStampInFrame']
        self.messageSize = params['messageSize']
        self.mountPoint = params['mountPoint']
        self.messageType = params['messageType']
        self.decodedFrame = []
        self.decodedObs = []
        self.table = []
        self.satType = []

    def decode(self):
        rtcmmessage = Rtcm3()
        try:
            try:
                # logging.info(f"Decoding MSM message type {self}")
                self.satType, self.table = SAT_TYPE_TABLE_DICT.get(self.messageType)
                if self.data is None and not self.data:
                    return {"decodedObs": [], "table": self.table}
            except Exception as error:
                logging.error(f"Failed to get SAT_TYPE_TABLE_DICT {error}")
            if self.messageType >= 1071 and self.messageType <= 1127:
                obsEpochStr = Decoder.gnssEpochStr(self.messageType, self.data[0][2] / 1000.0, 1)
                satSignals = rtcmmessage.msmSignalTypes(self.messageType, self.data[0][10])
                signalCount = len(satSignals)
                availSatMask = str(self.data[0][9])
                satId = [
                    f"{self.satType}{id + 1:02d}"
                    for id in range(64)
                    if availSatMask[id] == "1"
                ]
                messageType_mod = self.messageType % 10
                if messageType_mod == 5:
                    codeFineScaling = pow(2, -24)
                    phaseFineScaling = pow(2, -29)
                    snrScaling = 1
                elif messageType_mod == 7:
                    codeFineScaling = pow(2, -29)
                    phaseFineScaling = pow(2, -31)
                    snrScaling = pow(2, -4)
                availObsNo = 0
                availObsMask = str(self.data[0][11])
                allObs = []

                for satNo, sat in enumerate(self.data[1]):
                    satRoughRange = sat[0] + sat[2] / 1024.0
                    satRoughRangeRate = sat[3]

                    for signalNo, satSignal in enumerate(satSignals):
                        if availObsMask[satNo * signalCount + signalNo] == "1":
                            obsCode = (
                                satRoughRange
                                + self.data[2][availObsNo][0] * codeFineScaling
                            )
                            obsPhase = (
                                satRoughRange
                                + self.data[2][availObsNo][1] * phaseFineScaling
                            )
                            obsLockTimeIndicator = self.data[2][availObsNo][2]
                            obsSnr = self.data[2][availObsNo][4] * snrScaling
                            obsDoppler = (
                                satRoughRangeRate + self.data[2][availObsNo][5] * 0.0001
                            )
                            self.decodedObs.append((
                                    self.mountPoint,
                                    obsEpochStr,
                                    self.messageType,
                                    satId[satNo],
                                    satSignal,
                                    obsCode,
                                    obsPhase,
                                    obsDoppler,
                                    obsSnr,
                                    obsLockTimeIndicator,
                            ))
                            availObsNo += 1
        except Exception as error:
            logging.error(f"Failed to decode MSM frame with error: {error}. Setting observation to None")
            self.decodedObs = None
        return {"decodedObs": self.decodedObs, "table": self.table}

SAT_TYPE_TABLE_DICT = {
    1001: ("G", "gps"),
    1002: ("G", "gps"),
    1003: ("G", "gps"),
    1004: ("G", "gps"),
    1005: (" ", "coordinates"),
    1006: (" ", "coordinates"),
    1071: ("G", "gps"),
    1072: ("G", "gps"),
    1073: ("G", "gps"),
    1074: ("G", "gps"),
    1075: ("G", "gps"),
    1076: ("G", "gps"),
    1077: ("G", "gps"),
    1009: ("R", "glonass"),
    1010: ("R", "glonass"),
    1011: ("R", "glonass"),
    1012: ("R", "glonass"),
    1081: ("R", "glonass"),
    1082: ("R", "glonass"),
    1083: ("R", "glonass"),
    1084: ("R", "glonass"),
    1085: ("R", "glonass"),
    1086: ("R", "glonass"),
    1087: ("R", "glonass"),
    1091: ("E", "galileo"),
    1092: ("E", "galileo"),
    1093: ("E", "galileo"),
    1094: ("E", "galileo"),
    1095: ("E", "galileo"),
    1096: ("E", "galileo"),
    1097: ("E", "galileo"),
    1101: ("S", "sbas"),
    1102: ("S", "sbas"),
    1103: ("S", "sbas"),
    1104: ("S", "sbas"),
    1105: ("S", "sbas"),
    1106: ("S", "sbas"),
    1107: ("S", "sbas"),
    1111: ("J", "qzss"),
    1112: ("J", "qzss"),
    1113: ("J", "qzss"),
    1114: ("J", "qzss"),
    1115: ("J", "qzss"),
    1116: ("J", "qzss"),
    1117: ("J", "qzss"),
    1121: ("C", "beidou"),
    1122: ("C", "beidou"),
    1123: ("C", "beidou"),
    1124: ("C", "beidou"),
    1125: ("C", "beidou"),
    1126: ("C", "beidou"),
    1127: ("C", "beidou"),
}

DECODER_MAP = {
    1005: DecoderPOS,
    1006: DecoderPOS,
    1071: DecoderMSM,
    1072: DecoderMSM,
    1073: DecoderMSM,
    1074: DecoderMSM,
    1075: DecoderMSM,
    1076: DecoderMSM,
    1077: DecoderMSM,
    1078: DecoderMSM,
    1079: DecoderMSM,
    1080: DecoderMSM,
    1081: DecoderMSM,
    1082: DecoderMSM,
    1083: DecoderMSM,
    1084: DecoderMSM,
    1085: DecoderMSM,
    1086: DecoderMSM,
    1087: DecoderMSM,
    1088: DecoderMSM,
    1089: DecoderMSM,
    1090: DecoderMSM,
    1091: DecoderMSM,
    1092: DecoderMSM,
    1093: DecoderMSM,
    1094: DecoderMSM,
    1095: DecoderMSM,
    1096: DecoderMSM,
    1097: DecoderMSM,
    1098: DecoderMSM,
    1099: DecoderMSM,
    1100: DecoderMSM,
    1101: DecoderMSM,
    1102: DecoderMSM,
    1103: DecoderMSM,
    1104: DecoderMSM,
    1105: DecoderMSM,
    1106: DecoderMSM,
    1107: DecoderMSM,
    1108: DecoderMSM,
    1109: DecoderMSM,
    1110: DecoderMSM,
    1111: DecoderMSM,
    1112: DecoderMSM,
    1113: DecoderMSM,
    1114: DecoderMSM,
    1115: DecoderMSM,
    1116: DecoderMSM,
    1117: DecoderMSM,
    1118: DecoderMSM,
    1119: DecoderMSM,
    1120: DecoderMSM,
    1121: DecoderMSM,
    1122: DecoderMSM,
    1123: DecoderMSM,
    1124: DecoderMSM,
    1125: DecoderMSM,
    1126: DecoderMSM,
    1127: DecoderMSM,
}