import numpy as np
import pandas as pd
import sqlite3 as sq
import urllib.request as url
import os
import yaml
from urllib.error import HTTPError

######################################################################
#	Database Management v1.0                                     #
#	2022-10-15                                                   #
#                                                                    #
#                                                                    #
#	Author: Mike Rehnberg feat. Lots of Help from Jake Wimberly  #
#                                                                    #
#                                                                    #
######################################################################
class DBManager:


	def __init__(self, dbfile, db_config):
#		self.constants = {
#			'rootSrc': '/home/michael.rehnberg/GEPSSoundings/grib/',
#			'dbname': dbfile,
#			'archive': 'grib',
#			'leftlon': 95,
#			'rightlon': 100,
#			'toplat': 37,
#			'bottomlat': 34,
#		}
#		self.models = {
#			'GEPS': {
#				'members': 20,
#				'fHours': 120,
#				'memberName': 'cmc_gep',
#				'controlName': 'cmc_gec',
#				'increment': 12,
#				'base': 0,
#			},
#			'GEFS': {
#				'members': 30,
#				'fHours': 120,
#				'memberName': 'gep',
#				'controlName': 'gec',
#				'increment': 12,
#				'base': 0,
#			},
#		}
#		self.urlPatterns = {
#			'GEPS': 'https://nomads.ncep.noaa.gov/cgi-bin/filter_cmcens.pl?file={model_name:s}.t{init_time:02d}z.pgrb2a.0p50.f{fhour:03d}&lev_1000_mb=on&lev_100_mb=on&lev_10_mb=on&lev_200_mb=on&lev_250_mb=on&lev_500_mb=on&lev_50_mb=on&lev_700_mb=on&lev_850_mb=on&lev_925_mb=on&var_RH=on&var_TMP=on&var_UGRD=on&var_VGRD=on&var_VVEL=on&subregion=&leftlon={leftlon:f}&rightlon={rightlon:f}&toplat={toplat:f}&bottomlat={bottomlat:f}&dir=%2Fcmce.{yyyymmdd:s}%2F{ztime:02d}%2Fpgrb2ap5',
#			'GEFS': 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gefs_atmos_0p50a.pl?file={model_name:s}.t{init_time:02d}z.pgrb2a.0p50.f{fhour:03d}&lev_1000_mb=on&lev_100_mb=on&lev_10_mb=on&lev_200_mb=on&lev_250_mb=on&lev_300_mb=on&lev_500_mb=on&lev_50_mb=on&lev_700_mb=on&lev_850_mb=on&lev_925_mb=on&var_RH=on&var_TMP=on&var_UGRD=on&var_VGRD=on&var_VVEL=on&subregion=&leftlon={leftlon:f}&rightlon={leftlon:f}&toplat={leftlon:f}&bottomlat={leftlon:f}&dir=%2Fgefs.{yyyymmdd:s}%2F{ztime:02d}%2Fatmos%2Fpgrb2ap5',
#		}
		with open(db_config, 'r') as configFile:
			config = yaml.safe_load(configFile)
		self.constants = config['constants']
		self.constants['dbname'] = dbfile
		self.models = config['models']
		self.urlPatterns = config['urlPatterns']
		self.conn = sq.connect(self.constants['dbname'])
		self.curs = self.conn.cursor()
		self.curs.execute('CREATE TABLE if not exists grib (model TEXT, cycle TEXT, member INTEGER, fhour INTEGER, path TEXT)')

	# returns a listing of all GRIB files currently available
	# results returned as a list of tuples...where each tuple is a single record from the database, structured as (model, cycle, member, forecast-hour, file-path)
	def listAllGribs(self):
		#db = self.constants['dbname']
		#table = self.constants['archive']
		#conn = sq.connect(db)
		#c = conn.cursor()
		sqlString = 'SELECT * FROM ' + self.constants['archive'] + ' ORDER BY cycle ASC' # build appropriate SQL query
		self.curs.execute(sqlString)
		result = self.curs.fetchall()
		if result is None:
			raise ValueError('There is no data in the specified database.')
		return result


	# returns the record of the latest GRIB file stored in the database
	def getLatestGrib(self, model=None):
		if model is None:
			sqlString = "SELECT DISTINCT grib.model as gm, grib.cycle as gc FROM grib INNER JOIN ( SELECT model, MAX(cycle) as cycle FROM grib) g ON gm=g.model AND gc=g.cycle" # build appropriate SQL query...does an inner join in order to get the most recent run for each model
		else:
			sqlString = "SELECT DISTINCT grib.model as gm, grib.cycle as gc FROM grib INNER JOIN ( SELECT model, MAX(cycle) as cycle FROM grib WHERE model='" + model + "') g ON gm=g.model AND gc=g.cycle"
		# if a model is specified, add another wrapper query to subset to just the requested model

		self.curs.execute(sqlString)
		result = self.curs.fetchall()
		if result is None:
			raise ValueError('There is no data for the requested model present in the specified database.')
		return result


	# returns the name of the requested model in NOMADS GRIB naming format (i.e. gec00, gep03, etc.)
	def __makeMemberString(self, model, member):
		# for both GEPS and GEFS, the control has a different syntax from each of the perturbation members
		if member == -1:
			return self.models[model]['controlName'] + '00'
		else:
			return self.models[model]['memberName'] + str(member).zfill(2)


	# returns the name of a GRIB file, formatted via **** THIS SOFTWARE'S **** naming convention, *not* NOMADS convention
	# the NOMADS model naming convention is incorporated in this, but will be different from, say, a NOMADS URL filename
	def __gribName(self, model, cycle, hour, member, fHour):
		# YYYYMMDD.HHz.fXXXX.
		# ex 20220918.12z.f009.gep02.grib
		memberString = self.__makeMemberString(model, member)
	
		return model + '.' + cycle + '.' + str(hour).zfill(2) + '.f' + str(fHour).zfill(3) + '.' + memberString + '.grib'


	# given a model, run, forecast hour, and member number, generates the URL to access that GRIB file on NOMADS
	def __makeWebPath(self, model, cycle, hour, member, fHour):
		urlPatternBase = self.urlPatterns[model] # url patterns are stored in the class lookup table
		modelUrlName = self.__makeMemberString(model, member)
		#if member == -1:
		#	modelUrlName = self.models[model]['controlName']
		#else:
		#	modelUrlName = self.models[model]['memberName'] + str(member).zfill(2)
		urlPattern = urlPatternBase.format(model_name=modelUrlName, init_time=hour, fhour=fHour, leftlon=self.constants['leftlon'], rightlon=self.constants['rightlon'], toplat=self.constants['toplat'], bottomlat=self.constants['bottomlat'], yyyymmdd=cycle, ztime=hour) # done via built-in Pythonic string formatting/substitution
		return urlPattern


	# given a model, run, forecase hour, and member number, generates the path to that hypothetical GRIB file
	def __makeLocalPath(self, model, cycle, hour, member, fHour):
		fileName = self.__gribName(model, cycle, hour, member, fHour)
		cyclec = cycle + str(hour).zfill(2)
		localPath = model + '/' + cyclec + '/' + fileName
		return self.constants['rootSrc'] + localPath


	# returns true if particular GRIB already exists in database, false if not
	# returns path to file, if it exists, otherwise returns None
	def checkForFile(self, model, cycle, hour, member, fHour):
		cyclec = cycle + str(hour).zfill(2)
		sqlString = "SELECT path FROM " + self.constants['archive'] + " WHERE model='" + model + "' AND cycle='" + cyclec + "' AND member = '" + str(member) + "' and fhour = '" + str(fHour).zfill(3) + "'" # build appropriate SQL string

		self.curs.execute(sqlString)
		result = self.curs.fetchall()

		#print(result)

		# if we get a list of len > 0 then we know the file exists, so just return the file path
		# otherwise, return False but also return the hypothetical path for IF the file did exist...so that this method can be flexible for creating a file that doesn't exist
		if len(result) > 0:
			return (True, result[0][0])
		else:
			return (False, self.__makeLocalPath(model, cycle, hour, member, fHour))
	

	# returns path to appropriate GRIB2 file which was just created
	# raises ValueError when GRIB isn't available from NOMADS
	def downloadGrib(self, model, cycle, hour, member, fHour):
		#filePath = self.makeLocalPath(model, run, fHour, member) # create file path based on input vars
		fileExists, filePath = self.checkForFile(model, cycle, hour, member, fHour) # check if that path exists already
		# if it exists, just return the path...otherwise download it and then return the path
		if fileExists and os.path.exists(filePath[0]):
			return filePath
		else:
			gribUrl = self.__makeWebPath(model, cycle, hour, member, fHour) # create web path to download GRIB
			#print(fileExists)
			#print(filePath)
			os.makedirs(os.path.dirname(filePath), exist_ok=True) # os.makedirs ensures that the directory we need is created if it doesn't already exist

			# download appropriate GRIB from NOMADS
			try: 
				with url.urlopen(gribUrl, None, 30) as mike:
					with open(filePath, 'wb') as gribby:
						gribby.write(mike.read()) # first, write the GRIB file to disk
						self.__createNewGrib(model, cycle, hour, member, fHour) # second, create record of the GRIB we just created in the database
			except HTTPError as hte:
				#raise ValueError('Specified GRIB not available for download.')
				#return None
				print(filePath)
			return filePath


	# downloads all forecast hours for a certain model and member
	def downloadModel(self, model, cycle, hour, member=None):
		maxFHour = self.models[model]['fHours'] # set up loop to stop at the final forecast hour specified in configuration
		inc = self.models[model]['increment'] # set up loop to increment by specified number of forecast hours in configuration

		# loop through every forecast hour 
		for i in range(0, maxFHour, inc):
			# if no member is specified, we're looping through EVERY member of the ensemble...
			if member is None:
				# download control member
				try:
					self.downloadGrib(model, cycle, hour, -1, i)
				except ValueError as v:
					raise ValueError(str(v))
				# loop through every member if no member is selected
				for j in range(1,self.models[model]['members']+1):
					print(j)
					try:
						self.downloadGrib(model, cycle, hour, j, i)
					except ValueError as v:
						raise ValueError(str(v))
			else:
				try: 
					self.downloadGrib(model, cycle, hour, member, i) # otherwise just download the requested member
				except ValueError as v:
					raise ValueError(str(v))


	# creates a new entry in grib database with details of a particular GRIB file
	# 'private' method which should only be run within downloadGrib(), after a GRIB has been successfully downloaded, to preserve db
	def __createNewGrib(self, model, cycle, hour, member, fHour):
		filePath = self.__makeLocalPath(model, cycle, hour, member, fHour) # start by getting the path to the file

		#db = self.constants['dbname']
		#table = self.constants['archive']
		#conn = sq.connect(db)
		#c = conn.cursor()

		cyclec = cycle + str(hour).zfill(2)
		modelTuple = [(model, cyclec, member, fHour, filePath)]
		sqlString = 'INSERT INTO ' + self.constants['archive'] + ' VALUES (?, ?, ?, ?, ?)'

		# uses SQLite's executemany() function, which I have only recently discovered but which I understand as follows
		# replace each individual field's value with a ? in records denoted by ( ... ) in the SQL command
		# pass in an array of tuples, with each tuple being one of the records to insert, and each value in a tuple being a field value in the specified record
		# even if these details aren't 100% accurate...I mean it works, so like...
		self.curs.executemany(sqlString, modelTuple)
		self.conn.commit()


	# deletes all GRIB files with cycle ID older than a certain date # optionally, do this only for a specified model # notably enough  def deleteOldGribs(self, olderThan, model=None): sqlString = "SELECT path FROM " + self.constants['archive'] + " WHERE cycle < " + olderThan if model is not None: sqlString = sqlString + " AND model='" + model + "'" self.curs.execute(sqlString) result = self.curs.fetchall() if result is None: return else: for thing in result: os.remove(thing[0])

		sqlString = "DELETE FROM " + self.constants['archive'] + " WHERE cycle < " + olderThan
		if model is not None:
			sqlString = sqlString + " AND model='" + model + "'"
		self.curs.execute(sqlString)
		self.conn.commit()


	# deletes a single specified GRIB file
	def deleteGrib(self, model, cycle, hour, member, fHour):
		sqlString = "DELETE FROM " + self.constants['archive'] + " WHERE model='" + model + "' AND cycle='" + cycle + str(hour).zfill(2) + "' AND member='" + str(member) + "' AND fhour='" + str(fHour) + "'"
		self.curs.execute(sqlString)
		self.conn.commit()


	# returns a list of the unique model cycles available in the database
	def getModelCycles(self, model=None):
		sqlString = "SELECT DISTINCT cycle, model FROM " + self.constants['archive']
		if model is not None:
			sqlString = sqlString + " WHERE model='" + model + "'"
		self.curs.execute(sqlString)
		result = self.curs.fetchall()
		return result


	# queries NOMADS to see if a given model run is available
	# not the most elegant solution, but NOMADS doesn't appear to have an easy way to test if a GRIB exists
	def queryForGrib(self, model, cycle, hour, member=None, fHour=None):
		if member is None:
			member = -1
		if fHour is None:
			fHour = 0
		try:
			if member is None or fHour is None:
				fileExists = False
			else:
				fileExists, filePath = self.checkForFile(model, cycle, hour, member, fHour)
			self.downloadGrib(model, cycle, hour, member, fHour)
			if not fileExists:
				self.deleteGrib(model, cycle, hour, member, fHour)
			return True
		except ValueError as v:
			return False

#	def displayPathsTemp(self, model, cycle, hour, member, fHour):
#		print(self.__makeWebPath(model, cycle, hour, member, fHour))
#		print(self.__makeLocalPath(model, cycle, hour, member, fHour))




############### BUF-SPECIFIC METHODS #############

### did i just accidentally forget what i was doing and write the wrong api

### ...yes

### UPDATE these methods have been moved to a separate project BufBuilder as of 2022-09-20
