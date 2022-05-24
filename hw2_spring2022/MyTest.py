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
        self.assertEqual(Status.OK, Solution.addDisk(Disk(1, "DELL", 10, 11, 12)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addDisk(Disk(3, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)),
                         "ID 1 already exists")
        self.assertEqual(1, Solution.getDiskByID(1).getDiskID(), "Should work")
        self.assertEqual(10, Solution.getDiskByID(1).getSpeed(), "Should work")
        self.assertEqual(11, Solution.getDiskByID(1).getFreeSpace(), "Should work")
        self.assertEqual(12, Solution.getDiskByID(1).getCost(), "Should work")
        self.assertEqual("DELL", Solution.getDiskByID(1).getCompany(), "Should work")
        self.assertEqual(None, Solution.getDiskByID(9).getDiskID(), "ID 9 doesn't exists")
        self.assertEqual(Status.OK, Solution.deleteDisk(3), "Should work")
        self.assertEqual(Status.NOT_EXISTS, Solution.deleteDisk(9), "Should work")


    def test_RAM(self) -> None:
        self.assertEqual(Status.OK, Solution.addRAM(RAM(1, "Kingston", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAM(RAM(2, "Kingston", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addRAM(RAM(3, "Kingston", 10)), "Should work")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addRAM(RAM(2, "Kingston", 10)),
                         "ID 2 already exists")
        self.assertEqual(3, Solution.getRAMByID(3).getRamID(), "Should work")
        self.assertEqual(10, Solution.getRAMByID(3).getSize(), "Should work")
        self.assertEqual("Kingston", Solution.getRAMByID(3).getCompany(), "Should work")
        self.assertEqual(None, Solution.getRAMByID(9).getRamID(), "ID 9 doesn't exists")
        self.assertEqual(Status.OK, Solution.deleteRAM(3), "Should work")
        self.assertEqual(None, Solution.getRAMByID(3).getRamID(), "Should work")
        self.assertEqual(Status.NOT_EXISTS, Solution.deleteRAM(9), "Should work")



    def test_File(self) -> None:
        self.assertEqual(Status.OK, Solution.addFile(File(1, "wav", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(2, "wav", 10)), "Should work")
        self.assertEqual(Status.OK, Solution.addFile(File(3, "wav", 10)), "Should work")
        self.assertEqual(Status.ALREADY_EXISTS, Solution.addFile(File(3, "wav", 10)),
                         "ID 3 already exists")
        self.assertEqual(3, Solution.getFileByID(3).getFileID(), "Should work")
        self.assertEqual("wav", Solution.getFileByID(3).getType(), "Should work")
        self.assertEqual(10, Solution.getFileByID(3).getSize(), "Should work")
        self.assertEqual(None, Solution.getFileByID(9).getFileID(), "ID 9 doesn't exists")
        self.assertEqual(Status.OK, Solution.deleteFile(File(3, "wav", 10)), "Should work")
        self.assertEqual(None, Solution.getFileByID(3).getFileID(), "ID 3 doesn't exists")

# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
