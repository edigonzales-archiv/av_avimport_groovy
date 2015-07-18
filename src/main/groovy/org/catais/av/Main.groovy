package org.catais.av

import groovy.util.logging.Log4j2
import groovy.util.CliBuilder

@Log4j2
class Main {
	
	//TODO: proper exception handling!!!!
	
	static main(args) {

		// These must be configurable.
		def importDirectory = '/tmp/'
		//		
		
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
			_ longOpt: 'initdb', 'Intitialize databasy by creating empty schema', required: false
		}
        		
		def options= cli.parse(args)
		if (!options) {
			return
		}
		
		if (options.help) {
			cli.usage()
		}
		
		if (options.download) {
			log.info 'Download files from FTP Server.'
			
//			def ftp = new CataisFtp()
//			ftp.downloadDirectory = importDirectory
//			fileList = ftp.downloadData()
		
			log.debug "List of downloaded files: ${fileList}"
		}
		
		if (options.initdb) {
			log.info 'Create database schema.'
			
			def pg = new PostgresqlDatabase()
			pg.grantSelectToPublicDbusr = true
			pg.addAdditionalAttributes = true
			pg.createSchema()
		}
		
		
		
		
		
		endTime = Calendar.instance.time
		endTimeMs = endTime.time
		elapsedTime = (endTimeMs - startTimeMs)

		log.info "Elapsed time: ${elapsedTime} ms"
		log.info "End: ${endTime}."
		
		println "Stefan"
		
	}

}
