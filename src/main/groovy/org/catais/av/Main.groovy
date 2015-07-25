package org.catais.av

import groovy.util.logging.Log4j2

import java.security.cert.PKIXRevocationChecker.Option;

import groovy.util.CliBuilder

@Log4j2
class Main {
	
	//TODO: proper exception handling!!!!
	
	static main(args) {					
		def cli = new CliBuilder(
			usage: 'av_avimport', 
			header: '\nAvailable options (use --help for help):\n') 	 
		cli.with {
			_ longOpt: 'help', 'Usage Information'
			_ longOpt: 'download', 'Download data from FTP server.', args:1, argName:'directory'
			_ longOpt: 'schemaimport', 'Prepare database by creating schema with empty tables,', required: false
			_ longOpt: 'attributes', 'Add fosnr, lot and delivery date to each table/record.', required: false
			_ longOpt: 'grant', 'Grant usage/select to a public user/role,', args:1, argName:'role'
			_ longOpt: 'import', 'Import data into database.', args:1, argName:'directory'
		}
        		
		// If a non existing option is passed, 
		// every following (correct) option is set to false.
		def options= cli.parse(args)
				
		if (args.size() == 0) {
			cli.usage()
			return
		}
		
		if (!options) {
			cli.usage()
			return
		}
		
		if (options.help) {			
			cli.usage()
			return
		}
									
		def startTime = Calendar.instance.time
		def endTime
		log.info "Start: ${startTime}."
		
		if (options.download) {
			log.info 'Download files from FTP server:'
			
			def ftp = new CataisFtp()
			ftp.downloadDirectory = options.download
//			fileList = ftp.downloadData()
			
			// TODO: What do I do with fileList? Some QA at the end?
					
			endTime = Calendar.instance.time
			log.debug "Elapsed time: ${(endTime.time - startTime.time)} ms."
			log.info "All files downloaded from FTP server."			
		}
		
		if (options.schemaimport) {
			log.info 'Create database schema:'
			
			def pg = new PostgresqlDatabase()
			
			if (options.grant) {
				pg.grantPublic = options.grant
			}
			
			if (options.attributes) {
				pg.addAdditionalAttributes = true
			}
			
			pg.initSchema()
			
			endTime = Calendar.instance.time
			log.debug "Elapsed time: ${(endTime.time - startTime.time)} ms."
			log.info 'Database schema created.'			
		}
		
		if (options.import) {
			log.info 'Import data:'
			
			def pg = new PostgresqlDatabase()
			
			// We need to set this to true again
			// since we create new pg object.
			if (options.attributes) {
				pg.addAdditionalAttributes = true
			}
			
			// If we download the data first,
			// we want to use the download 
			// directory.
			if (options.download) {
				pg.runImport(options.download)
			} else {
				pg.runImport(options.import)
			}
			
			endTime = Calendar.instance.time
			log.debug "Elapsed time: ${(endTime.time - startTime.time)} ms."
			log.info 'Importing done.'
		}
		
		endTime = Calendar.instance.time
		log.debug "Total elapsed time: ${(endTime.time - startTime.time)} ms"
		log.info "End: ${endTime}."		
	}
}
