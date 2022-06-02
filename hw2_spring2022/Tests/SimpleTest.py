import unittest
import Solution
from Utility.Status import Status
from Tests.abstractTest import AbstractTest
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk

'''
    Simple test, create one of your own
    make sure the tests' names start with test_
'''


class Test(AbstractTest):
    def test_Disk(self) -> None:
        self.assertEqual(Status.OK, Solution.addDisk(
            Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(
            Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(
            Disk(3, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)),
                         "ID 1 already exists")

    def test_RAM(self) -> None:
        self.assertEqual(Status.OK, Solution.addRAM(
            RAM(1, "Kingston", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAM(
            RAM(2, "Kingston", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAM(
            RAM(3, "Kingston", 10)), "Should work")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addRAM(RAM(2, "Kingston", 10)),
                         "ID 2 already exists")

    def test_File(self) -> None:
        self.assertEqual(Status.OK, Solution.addFile(
            File(1, "wav", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(
            File(2, "wav", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(
            File(3, "wav", 10)), "Should work")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addFile(File(3, "wav", 10)),
                         "ID 3 already exists")

    def test_CloseFiles(self) -> None:
        files = [
            File(fileID=1, type="xml", size=10),
            *[File(fileID=i, type="rst", size=10) for i in range(2, 15)],
        ]
        disks = [
            Disk(diskID=i, company="DELL", speed=100*i+i, cost=10+i, free_space=1000*i) for i in range(1, 21)
        ]

        for i, file in enumerate(files):
            self.assertEqual(Status.OK, Solution.addFile(file), "Should work")
        for i, disk in enumerate(disks):
            self.assertEqual(Status.OK, Solution.addDisk(disk), "Should work")

        # Add fileId 1 to disks 1-9
        for i in range(9):
            self.assertEqual(Status.OK, Solution.addFileToDisk(
                files[0], disks[i].getDiskID()), "Should work")

        # Add fileId 2 to disks 4-6
        for i in range(3, 6):
            self.assertEqual(Status.OK, Solution.addFileToDisk(
                files[1], disks[i].getDiskID()), "Should work")

        # Add fileId 3 to disks 3-6
        for i in range(2, 6):
            self.assertEqual(Status.OK, Solution.addFileToDisk(
                files[2], disks[i].getDiskID()), "Should work")

        # Add fileId 4 to disks 2-6
        for i in range(1, 6):
            self.assertEqual(Status.OK, Solution.addFileToDisk(
                files[3], disks[i].getDiskID()), "Should work")

        # Add fileId 5 to disks 6-10
        for i in range(5, 10):
            self.assertEqual(Status.OK, Solution.addFileToDisk(
                files[4], disks[i].getDiskID()), "Should work")

        # Get list of closest files
        self.assertEqual([4], Solution.getCloseFiles(
            files[0].getFileID()), "Should work")
        self.assertListEqual([1, 2, 3, 4, 5, 7, 8, 9, 10, 11], # fileID 6 isn't close to itself
                             Solution.getCloseFiles(files[5].getFileID()), "Should work")


# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
