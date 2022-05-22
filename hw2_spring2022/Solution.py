from typing import List
import Utility.DBConnector as Connector
from Utility.Status import Status
from Utility.Exceptions import DatabaseException
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        createFiles = "CREATE TABLE Files(FileId INTEGER PRIMARY KEY NOT NULL," \
                      "Type TEXT NOT NULL," \
                      "DiskSizeNeeded INTEGER NOT NULL, " \
                      "CHECK ( FileId >0 AND DiskSizeNeeded >=0));"

        createDisks = "CREATE TABLE Disks(DiskId INTEGER PRIMARY KEY NOT NULL," \
                      "ManufacturingCompany TEXT NOT NULL," \
                      "Speed INTEGER NOT NULL," \
                      "FreeSpace INTEGER NOT NULL," \
                      "CostPerByte INTEGER NOT NULL, " \
                      "CHECK ( DiskId>0 AND Speed>0 AND CostPerByte>0 AND FreeSpace>=0));"

        createRams = "CREATE TABLE Rams(RamId INTEGER PRIMARY KEY NOT NULL," \
                     "Size INTEGER NOT NULL," \
                     "Company TEXT NOT NULL," \
                     "CHECK ( RamId >0 AND Size>0 ));"

        createFilesOnDisks = "CREATE TABLE FilesOnDisks(DiskId INTEGER NOT NULL," \
                             "FileId INTEGER NOT NULL," \
                             "FOREIGN KEY (DiskId) REFERENCES Disks(DiskId) ON DELETE CASCADE," \
                             "FOREIGN KEY (FileId) REFERENCES Files(FileId) ON DELETE CASCADE," \
                             "UNIQUE(DiskId,FileId));"

        createRamsOnDisks = "CREATE TABLE RamsOnDisks(DiskId INTEGER NOT NULL," \
                            "RamId INTEGER NOT NULL," \
                            "FOREIGN KEY (DiskId) REFERENCES Disks(DiskId) ON DELETE CASCADE," \
                            "FOREIGN KEY (RamId) REFERENCES Rams(RamId) ON DELETE CASCADE," \
                            "UNIQUE(DiskId,RamId));"

        query = sql.SQL(f"BEGIN; "
                        f"{createFiles}"
                        f"{createDisks}"
                        f"{createRams}"
                        f"{createFilesOnDisks}"
                        f"{createRamsOnDisks}"
                        f"COMMIT;")
        conn.execute(query)
        conn.commit()
    except Exception as e:
        print(e)
        return Status.ERROR
    finally:
        if conn:
            conn.close()
    return Status.OK


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        clearFiles = "DELETE FROM Files CASCADE;"

        clearDisks = "DELETE FROM Disks CASCADE;"

        clearRams = "DELETE FROM Rams CASCADE;"

        clearFilesOnDisks = "DELETE FROM FilesOnDisks;"

        clearRamsOnDisks = "DELETE FROM RamsOnDisks;"

        query = sql.SQL(f"BEGIN; "
                        f"{clearFiles}"
                        f"{clearDisks}"
                        f"{clearRams}"
                        f"{clearFilesOnDisks}"
                        f"{clearRamsOnDisks}"
                        f"COMMIT;")
        conn.execute(query)
        conn.commit()
    except DatabaseException as e:
        return Status.ERROR
    finally:
        if conn:
            conn.close()
    return Status.OK


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        clearFiles = "DROP TABLE Files CASCADE;"

        clearDisks = "DROP TABLE Disks CASCADE;"

        clearRams = "DROP TABLE Rams CASCADE;"

        clearFilesOnDisks = "DROP TABLE FilesOnDisks;"

        clearRamsOnDisks = "DROP TABLE RamsOnDisks;"

        query = sql.SQL(f"BEGIN; "
                        f"{clearFiles}"
                        f"{clearDisks}"
                        f"{clearRams}"
                        f"{clearFilesOnDisks}"
                        f"{clearRamsOnDisks}"
                        f"COMMIT;")
        conn.execute(query)
        conn.commit()
    except DatabaseException as e:
        return Status.ERROR
    finally:
        if conn:
            conn.close()
    return Status.OK


def addFile(file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Files VALUES({FileId}, {Type}, {DiskSizeNeeded})").format(
            FileId=sql.Literal(file.getFileID()),
            Type=sql.Literal(file.getType()),
            DiskSizeNeeded=sql.Literal(file.getSize()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        return Status.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return Status.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION:
        return Status.BAD_PARAMS
    except Exception as e:
        return Status.ERROR
    finally:
        if conn:
            conn.close()
    return Status.OK


def getFileByID(fileID: int) -> File:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Files WHERE FileId = {FileId}").format(FileId=sql.Literal(fileID))
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return File.badFile()
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return File.badFile()
    return File(result[0]['fileid'], result[0]['type'], result[0]['disksizeneeded'])


def deleteFile(file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Files WHERE FileId = {FileId} AND Type={Type} AND DiskSizeNeeded={Size}").format(
            FileId=sql.Literal(file.getFileID()),
            Type=sql.Literal(file.getType()),
            Size=sql.Literal(file.getSize()))

        conn.execute(query)
        conn.commit()
    except DatabaseException as e:
        return Status.ERROR
    finally:
        if conn:
            conn.close()
    return Status.OK


def addDisk(disk: Disk) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Disks VALUES({DiskId} ,{ManufacturingCompany} ,{Speed}, {FreeSpace}, {CostPerByte})").format(
            DiskId=sql.Literal(disk.getDiskID()),
            ManufacturingCompany=sql.Literal(disk.getCompany()),
            Speed=sql.Literal(disk.getSpeed()),
            FreeSpace=sql.Literal(disk.getFreeSpace()),
            CostPerByte=sql.Literal(disk.getCost()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        return Status.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return Status.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION:
        return Status.BAD_PARAMS
    except Exception as e:
        return Status.ERROR
    finally:
        if conn:
            conn.close()
    return Status.OK


def getDiskByID(diskID: int) -> Disk:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Disks WHERE DiskId = {diskID}").format(diskID=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return Disk.badDisk()
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return Disk.badDisk()
    return Disk(result[0]['diskid'], result[0]['manufacturingcompany'], result[0]['Speed'], result[0]['freespace'],
                result[0]['costperbyte'])


def deleteDisk(diskID: int) -> Status:
    return Status.OK


def addRAM(ram: RAM) -> Status:
    return Status.OK


def getRAMByID(ramID: int) -> RAM:
    return RAM()


def deleteRAM(ramID: int) -> Status:
    return Status.OK


def addDiskAndFile(disk: Disk, file: File) -> Status:
    return Status.OK


def addFileToDisk(file: File, diskID: int) -> Status:
    return Status.OK


def removeFileFromDisk(file: File, diskID: int) -> Status:
    return Status.OK


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    return Status.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    return Status.OK


def averageFileSizeOnDisk(diskID: int) -> float:
    return 0


def diskTotalRAM(diskID: int) -> int:
    return 0


def getCostForType(type: str) -> int:
    return 0


def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getFilesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getCloseFiles(fileID: int) -> List[int]:
    return []
