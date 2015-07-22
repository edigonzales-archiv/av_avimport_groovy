package org.catais.av

import groovy.util.logging.Log4j2
import groovy.util.CliBuilder

@Log4j2
class Main {
	
	//TODO: proper exception handling!!!!
	
	static main(args) {

		def importDirectory = '/tmp/'		
		def fileList
		
		def startTime = Calendar.instance.time
		def startTimeMs = startTime.time

		def endTime   
		def endTimeMs  
		def elapsedTime 
		
		log.info "Start: ${startTime}."
	
		def cli = new CliBuilder(
			usage: 'groovy Fubar.groovy --initdb', 
			header: '\nAvailable options (use --help for help):\n') 	 
		cli.with {
			_ longOpt: 'help', 'Usage Information'
			_ longOpt: 'download', 'Download data from FTP server'
			_ longOpt: 'schemaimport', 'Prepare database by creating schema with empty tables', required: false
			_ longOpt: 'additionalattributes', 'Add fosnr, lot and delivery date to each table/record.', required: false
			_ longOpt: 'grantpublic', 'Grant usage/select to a public user/role', args:1, argName:'role'
			_ longOpt: 'importdirectory', 'Directory with data to import. Will be used as download directory too.', args:1, argName:'directory'
			_ longOpt: 'import', 'Import data into database'
		}
        		
		def options= cli.parse(args)
		if (!options) {
			return
		}
		
		if (options.help) {			
			cli.usage()
			return
		}
		
		if (options.importdirectory) {
			importDirectory = options.importdirectory
		}
		
		if (options.download) {
			log.info 'Download files from FTP server.'
			
			def ftp = new CataisFtp()
			ftp.downloadDirectory = importDirectory
//			fileList = ftp.downloadData()
			
			// TODO: What do I do with fileList? Some QA at the end?
		
			log.debug "List of downloaded files: ${fileList}"
			
			elapsedTime = Utils.elapsedTime(startTimeMs)
			log.info "Elapsed time: ${elapsedTime} ms"
			log.info "All files downloaded from FTP server."			
		}
		
		if (options.schemaimport) {
			log.info 'Create database schema:'
			
			def pg = new PostgresqlDatabase()
			
			if (options.grantpublic) {
				pg.grantPublic = options.grantpublic
			}
			
			if (options.additionalattributes) {
				pg.addAdditionalAttributes = true
			}
			
//			pg.initSchema()
			
			elapsedTime = Utils.elapsedTime(startTimeMs)
			log.info "Elapsed time: ${elapsedTime} ms"
			log.info 'Database schema created.'			
		}
		
		if (options.import) {
			log.info 'Import data:'
			
			def pg = new PostgresqlDatabase()
			pg.runImport(importDirectory)
			
			elapsedTime = Utils.elapsedTime(startTimeMs)
			log.info "Elapsed time: ${elapsedTime} ms"
			log.info 'Importing done.'
		}
		
		elapsedTime = Utils.elapsedTime(startTimeMs)
		log.info "Total elapsed time: ${elapsedTime} ms"
		log.info "End: ${endTime}."
		
		println "Stefan"	
	}
}
