import numpy as np
import pandas as pd
import DBManager as dbm
import pytz

import EnsCalculator as ec

def updateDatabase():
	# first, look in the grib db to see what GRIBs are available
	gribs = manager.getModelCycles()

	# second, look in the img db to see what images exist
	images = manager.getImageCycles()

	# for any available GRIBs that don't have corresponding images, generate the images
	missingImages = []
	for grib in gribs:
		if grib not in images:
			missingImages.append(grib)

	for thing in missingImages:
		

	# delete old images

#execute
def main():
	manager = dbm.DBManager('test_config.yml')
	
	print('Updating GEFS image data...')
	deleted_gefs_imgs = updateDatabase(manager, 'GEFS')

	print('Updating GEPS image data...')
	deleted_geps_imgs = updateDatabase(manager, 'GEPS')

	print('Images should now be up-to-date')

if __name__ == "__main__":
	main()
