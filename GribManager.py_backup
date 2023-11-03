import numpy as np
import pandas as pd
import DBManager as dbm
import pytz

import sys

from datetime import datetime, timedelta, timezone

### CONFIGURATION

hourLimit=20 # the number of hours to look back in the past

###

# cycles through 3-day old run up through the present, and determines what model cycles are available on NOMADS
def getNomadsCycles(manager, model):
	cycleInterval = manager.models[model]['increment']
	cycleBase = manager.models[model]['base']
	
	currentTime = datetime.utcnow().replace(tzinfo=timezone.utc)
	earlierTime = currentTime - timedelta(hours=hourLimit)
	cyclesCompleted = int(earlierTime.hour / cycleInterval)
	startTime = datetime(year=earlierTime.year, month=earlierTime.month, day=earlierTime.day, hour=cycleBase + cycleInterval*cyclesCompleted, tzinfo=timezone.utc)
	
	availableRuns = {}
	while startTime <= currentTime:
		# query NOMADS
		available = manager.queryForGrib(model, startTime.strftime('%Y%m%d'), int(startTime.strftime('%H')))
		availableRuns[startTime.strftime('%Y%m%d%H')] = available # true or false for a given model cycle

		startTime = startTime + timedelta(hours=int(cycleInterval)) # once we hit the 'now()' just cut the loop
	return availableRuns

def updateDatabase(manager, model, text):
	localCycles = manager.getModelCycles(model) # gets the locally-stored cycles from the db
	nomadsCycles = getNomadsCycles(manager, model) # gets the available cycles from NOMADS

	# determine what cycles are missing from the local store
	if localCycles is None:
		missingCycles = list(nomadsCycles.keys())
	else:
		missingCycles = []
		for cycle in nomadsCycles.keys():
			if cycle not in localCycles:
				missingCycles.append(cycle)

	# if there are any missing cycles, attempt to download them
	if len(missingCycles) > 0:
		prompt = 'Preparing to download ' + model + ' runs '
		for cycle in missingCycles:
			prompt = prompt + cycle + ', '
		if text:
			resp = input(prompt + '...proceed?')
			if resp == 'n':
				return

		# download the missing runs
		for run in missingCycles:
			if text:
				print('Downloading run ' + str(run) + '\n')
			try:
				manager.downloadModel(model, run[0:8], int(run[8:]))
			except ValueError as v:
				print(str(v))
	
	# delete the old runs
	oldTime = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(hours=hourLimit+6)
	deleted = manager.deleteOldGribs(oldTime.strftime('%Y%m%d%H'))
	return deleted

def updateImages(manager, imgsToDelete=None):
	pass
	# first, figure out what images are missing

	# then, generate the proper images for all those files

	# finally, delete the images from old runs


# execute
def main():

	if len(sys.argv) >= 2 and sys.argv[1] == "1":
		text = True
	else:
		text = False

	manager = dbm.DBManager('/var/www/html/mike/DBManager/test_config.yml')

	# update the databases
	# TODO see how badly this f's up the time
	if text:
		print('Updating GEFS database...')
	deleted_gefs = updateDatabase(manager, 'GEFS', text)
        print(deleted_gefs)
	if text:
		print('Updating GEPS database...')
	deleted_geps = updateDatabase(manager, 'GEPS', text)
        print(deleted_geps)

	# generate new images
	#print('Generating missing images...')
	#updateImages(manager)
	
	if text:
		print('Database should now be up-to-date.')

if __name__ == "__main__":
	main()
