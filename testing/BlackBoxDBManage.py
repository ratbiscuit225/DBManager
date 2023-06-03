# For testing purposes, all methods should initially be supplied a DBManager() object with an empty database.

# Tests conducted successfully 2022-10-02

import DBManager as dbm

def populateDatabase(manager):
	manager.downloadGrib('GEFS', '20221001', 12, -1, 0)
	manager.downloadGrib('GEPS', '20221001', 0, -1, 0)
	return manager

def unpopulateDatabase(manager):
	manager.deleteGrib('GEFS', '20221001', 12, -1, 0)
	manager.deleteGrib('GEPS', '20221001', 0, -1, 0)
	return manager

def testListAllGribs(manager):
	# test empty database
	print('This test should print an empty database.')
	print(manager.listAllGribs())

	# add some GRIBs to the database and test again
	print('This test should print a few GEPS and GEFS runs.')
	manager = populateDatabase(manager)
	print(manager.listAllGribs())

	manager = unpopulateDatabase(manager)

def testGetLatestGrib(manager):
	# test result for an empty database
	print('This test should print an empty database.')
	print(manager.getLatestGrib())

	manager = populateDatabase(manager)

	# test result for a populated database
	print('This test should print 2022100112 run of the GEFS.')
	print(manager.getLatestGrib())

	# now test that even though GEFS run is later than GEPS run in DB, calling getLatestGrib() for GEPS returns the latest run of the GEPS only
	print('This test should print the 2022100100 run of the GEPS.')
	print(manager.getLatestGrib(model='GEPS'))

	manager = unpopulateDatabase(manager)

def testCheckForFile(manager):
	manager = populateDatabase(manager)

	# check for a file that's on the system
	print('This test should print True.')
	print(manager.checkForFile('GEFS', '20221001', 12, -1, 0))

	# check for a file that's not on the system
	print('This test should print False.')
	print(manager.checkForFile('GEPS', '20221001', 12, -1, 0))

	manager = unpopulateDatabase(manager)

def testDownloadGrib(manager):
	# try downloading a GRIB that's not available on NOMADS
	print('This test should print a ValueError.')
	try:
		path = manager.downloadGrib('GEFS', '20220901', 0, -1, 0)
		print(path)
	except ValueError as v:
		print(str(v))

	# try downloading a GRIB that's available on NOMADS
	print('This test should print a success message.')
	try:
		path = manager.downloadGrib('GEFS', '20221001', 0, 3, 0)
		print(path)
	except ValueError as v:
		print(str(v))

	# try downloading a GRIB that's available on NOMADS, but already downloaded locally as well
	print('This test should print a success message.')
	try:
		path = manager.downloadGrib('GEFS', '20221001', 0, -1, 0)
		print(path)
	except ValueError as v:
		print(str(v))

	manager = unpopulateDatabase(manager)

def testDownloadModel(manager):
	pass
	# for now have opted to forego testing of this method...it's well-documented, has very little comp logic, and doesn't have any boundary conditions that appear to be possible failure points

def testDeleteOldGribs(manager):
	manager = populateDatabase(manager)
	
	# try deleting data that's not there to delete
	print('This test should print nothing.')
	manager.deleteOldGribs(olderThan='2022091500')

	# try deleting data that's available to delete
	print('This test should print only the 2022100100 run of GEFS.')
	manager.deleteOldGribs(olderThan='2022100112')
	print(manager.listAllGribs())

	# try deleting data that's not available to delete
	print('This test should print an identical to result to the last one.')
	manager.deleteOldGribs(olderThan='2022093012')
	print(manager.listAllGribs())

	# try deleting data from only one model
	print('This test should print an identical result to the last one.')
	manager.deleteOldGribs(olderThan='2022100200', model='GEPS')
	print(manager.listAllGribs())
	print('This test should print an empty database.')
	manager.deleteOldGribs(olderThan='2022100200', model='GEFS')
	print(manager.listAllGribs())

def testDeleteGrib(manager):
	manager = populateDatabase(manager)

	# try deleting a specific GRIB stored locally
	print('This test should print only the 2022100100 GEPS run.')
	manager.deleteGrib('GEFS', '20221001', 12, -1, 0)
	print(manager.listAllGribs())
	
	# try deleting a specific GRIB that's not available locally
	print('This test should print an identical result to the last one.')
	manager.deleteGrib('GEFS', '20221001', 18, -1, 0)
	print(manager.listAllGribs())

	manager = unpopulateDatabase(manager)

def testGetModelCycles(manager):
	manager = populateDatabase(manager)

	# try getting all model cycles
	print('This test should print both GEFS and GEPS cycles.')
	print(manager.getModelCycles())

	# try getting specific model cycles
	print('This test should print only GEPS cycles.')
	print(manager.getModelCycles(model='GEPS'))
	
	manager = unpopulateDatabase(manager)

def testQueryForGrib(manager):
	manager = populateDatabase(manager)

	print(manager.listAllGribs())

	# try querying NOMADS for a GRIB that exists, without specifics
	print('This test should print True.')
	print(manager.queryForGrib('GEFS', '20221001', 12))

	# try querying NOMADS for a GRIB that exists, with specifics
	print('This test should return True.')
	print(manager.queryForGrib('GEFS', '20221001', 12, -1, 0))

	# try querying NOMADS for a GRIB that doesn't exist
	print('This test should return False.')
	print(manager.queryForGrib('GEPS', '20220915', 12))

	# ensure the method hasn't made any changes to the database
	print('This should look the same as it did at the beginning of this test.')
	print(manager.listAllGribs())

def main():
	manager = dbm.DBManager('testy.db', 'db_config.yml')
	print('\nTesting listAllGribs():')
	testListAllGribs(manager)
	print('\nTesting getLatestGrib():')
	testGetLatestGrib(manager)
	print('\nTesting checkForFile():')
	testCheckForFile(manager)
	print('\nTesting downloadGrib():')
	testDownloadGrib(manager)
	print('\nTesting downloadModel():')
	testDownloadModel(manager)
	print('\nTesting deleteOldGribs():')
	testDeleteOldGribs(manager)
	print('\nTesting deleteGrib():')
	testDeleteGrib(manager)
	print('\nTesting getModelCycles():')
	testGetModelCycles(manager)
	print('\nTesting queryForGrib():')
	testQueryForGrib(manager)

if __name__ == "__main__":
	main()
