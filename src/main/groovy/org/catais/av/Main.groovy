package org.catais.av

import groovy.util.logging.Log4j2
import groovy.util.CliBuilder

@Log4j2
class Main {

	static main(args) {
		
		def startTime = Calendar.instance.time
		def startTimeMs = startTime.time

		def endTime   
		def endTimeMs  
		def elapsedTime 
		
		log.info "Start import at ${startTime}."
	
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
			log.debug 'Download data'
			
			def ftp = new CataisFtp()
			ftp.downloadData()
			
			
		}
		
		if (options.initdb) {
			log.debug 'Initialize database'
		}
		
		
		
		
		
		endTime = Calendar.instance.time
		endTimeMs = endTime.time
		elapsedTime = (endTimeMs - startTimeMs)

		log.info "Elapsed time: ${elapsedTime} ms"
		log.info "Import done at ${endTime}."
		
		println "Stefan"
		
	}

}
