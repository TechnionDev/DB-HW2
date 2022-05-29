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
    # return  # TODO: Remove this
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DROP TABLE Files, Disks, Rams, FilesOnDisks, RamsOnDisks CASCADE;")
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

        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException as e:
        return Status.ERROR
    finally:
        if conn:
            conn.close()
    if rows_effected == 0:
        return Status.ERROR
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
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Disks WHERE DiskId = {diskID}").format(diskID=sql.Literal(diskID))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException as e:
        return Status.ERROR
    finally:
        if conn:
            conn.close()
    if rows_effected == 0:
        return Status.NOT_EXISTS
    return Status.OK


def addRAM(ram: RAM) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Rams VALUES({RamId}, {Size}, {Company})").format(
            RamId=sql.Literal(ram.getRamID()),
            Size=sql.Literal(ram.getSize()),
            Company=sql.Literal(ram.getCompany()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
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


def getRAMByID(ramID: int) -> RAM:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Rams WHERE ramID = {ramID}").format(ramID=sql.Literal(ramID))
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return RAM.badRAM()
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return RAM.badRAM()
    return RAM(result[0]['ramid'], result[0]['company'], result[0]['size'])


def deleteRAM(ramID: int) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Rams WHERE ramID = {ramID}").format(ramID=sql.Literal(ramID))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException as e:
        return Status.ERROR
    finally:
        if conn:
            conn.close()
    if rows_effected == 0:
        return Status.NOT_EXISTS
    return Status.OK


def addDiskAndFile(disk: Disk, file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        add_file = f"INSERT INTO Files VALUES ({file.getFileID()}, '{file.getType()}', {file.getSize()});"
        add_disk = f"INSERT INTO Disks VALUES ({disk.getDiskID()}, '{disk.getCompany()}', " \
                   f"{disk.getSpeed()}, {disk.getFreeSpace()}, {disk.getCost()});"
        query = sql.SQL(f"BEGIN; "
                        f"{add_disk} "
                        f"{add_file} "
                        f"COMMIT;")
        conn.execute(query)
        conn.commit()
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        return Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.rollback()
        return Status.ERROR
    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        return Status.ERROR
    finally:
        if conn:
            conn.close()
    return Status.OK


def addFileToDisk(file: File, diskID: int) -> Status:
    conn = None
    FilesOnDisks = f"INSERT INTO FilesOnDisks VALUES ({diskID},{file.getFileID()});"
    updateFreeSpace = f"UPDATE Disks SET FreeSpace = FreeSpace - {file.getSize()} WHERE DiskId = {diskID};"
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(f"BEGIN; "
                        f"{FilesOnDisks} "
                        f"{updateFreeSpace} "
                        f"COMMIT;")
        conn.execute(query)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        conn.rollback()
        return Status.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        return Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        return Status.BAD_PARAMS
    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        return Status.ERROR
    finally:
        if conn:
            conn.close()
    return Status.OK


def removeFileFromDisk(file: File, diskID: int) -> Status:
    conn = None
    updateFreeSpace = f"UPDATE Disks SET FreeSpace = FreeSpace + {file.getSize()} " \
                      f"WHERE DiskId = {diskID} AND EXISTS (SELECT * FROM FilesOnDisks WHERE " \
                      f"FileId = {file.getFileID()} AND DiskId = {diskID});"
    deleteFileFromDisk = f"DELETE FROM FilesOnDisks WHERE FileId = {file.getFileID()} AND DiskId = {diskID};"
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(f"BEGIN; "
                        f"{updateFreeSpace} "
                        f"{deleteFileFromDisk} "
                        f"COMMIT;")
        conn.execute(query)
        conn.commit()
    except DatabaseException:
        conn.rollback()
        return Status.ERROR
    finally:
        if conn:
            conn.close()
    return Status.OK


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    conn = None
    try:
        RamsOnDisks = f"INSERT INTO RamsOnDisks VALUES ({diskID},{ramID});"
        conn = Connector.DBConnector()
        query = sql.SQL(f"{RamsOnDisks}")
        conn.execute(query)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        conn.rollback()
        return Status.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        return Status.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid:
        conn.rollback()
        return Status.ERROR
    finally:
        if conn:
            conn.close()
    return Status.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(f"DELETE FROM RamsOnDisks WHERE RamId = {ramID} AND DiskId = {diskID};")
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException:
        conn.rollback()
        return Status.ERROR
    finally:
        if conn:
            conn.close()
    if rows_effected == 0:
        return Status.NOT_EXISTS
    return Status.OK


def averageFileSizeOnDisk(diskID: int) -> float:
    query = sql.SQL(f'SELECT AVG(DiskSizeNeeded) FROM Files f JOIN FilesOnDisks fod ON f.FileID = fod.FileID '
                    f'WHERE fod.DiskID = {diskID};')
    conn = None
    try:
        conn = Connector.DBConnector()
        _, result = conn.execute(query)
        conn.commit()
    except Exception:
        return -1
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return 0
    return result[0]['avg'] or 0


def diskTotalRAM(diskID: int) -> int:
    query = sql.SQL(f'SELECT SUM(Size) FROM Rams r JOIN RamsOnDisks rod ON r.RamID = rod.RamID '
                    f'WHERE rod.DiskID = {diskID};')
    conn = None
    try:
        conn = Connector.DBConnector()
        _, result = conn.execute(query)
        conn.commit()
    except Exception:
        return -1
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return 0
    return result[0]['sum'] or 0


def totalRAMOnDisk(diskID: int) -> int:
    return diskTotalRAM(diskID)


def getCostForType(type: str) -> int:
    query = sql.SQL(f'SELECT SUM(DiskSizeNeeded * CostPerByte) FROM Files JOIN Disks ON Type = \'{type}\';')
    conn = None
    try:
        conn = Connector.DBConnector()
        _, result = conn.execute(query)
        conn.commit()
    except Exception:
        return -1
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return 0
    return result[0]['sum'] or 0


def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    """
    Returns a list (max size is 5) of files' IDs that can be added to the disk with diskID as singles - not all together (even if they're on the disk).
    The list is sorted by fileIDs in descending order.
    """
    query = sql.SQL(f"""SELECT f.FileID FROM Files f
                            WHERE NOT EXISTS (SELECT fod.FileID FROM FilesOnDisks fod WHERE fod.DiskID = {diskID} AND f.FileID = fod.FileID) 
	                            AND EXISTS (SELECT d.DiskID FROM Disks d WHERE d.DiskID = {diskID} AND f.DiskSizeNeeded <= d.FreeSpace )
                            ORDER BY f.FileID DESC
                            LIMIT 5""")
    conn = None
    try:
        conn = Connector.DBConnector()
        _, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return []

    result = [x[0] for x in result.rows]
    return result


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
