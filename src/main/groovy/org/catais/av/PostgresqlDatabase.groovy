package org.catais.av

import groovy.util.logging.Log4j2
import groovy.sql.Sql
import java.sql.SQLException
import static groovy.io.FileType.*
import static groovy.io.FileVisitResult.*
import org.apache.commons.io.FilenameUtils

import ch.ehi.basics.logging.EhiLogger
import ch.ehi.ili2db.base.Ili2db
import ch.ehi.ili2db.base.Ili2dbException
import ch.ehi.ili2db.gui.Config
import ch.ehi.ili2pg.converter.PostgisGeometryConverter
import ch.ehi.sqlgen.generator_impl.jdbc.GeneratorPostgresql

// TODO: Write a method that returns all "real" tables. 
// We need this several times. And I think even for
// a manual rollback of an import. Easy when we have
// additional attributes: delete everythin where 
// "gem_bfs" IS NULL.
// Returning a list is enough?

@Log4j2
class PostgresqlDatabase {
	def dbhost = "localhost"
	def dbport = "5432"
	def dbdatabase = "xanadu2"
	def dbusr = "stefan"
	def dbpwd = "ziegler12"
	def dbschema = "av_avdpool_ng_arcs"
	def modelName = "DM01AVCH24D"
	
	// It worked on OS X w/o user and password. But there's a mess with the roles.
	// Ili2pg shows dburl so we should create another one for the groovy stuff.
	def dburl = "jdbc:postgresql://${dbhost}:${dbport}/${dbdatabase}?user=${dbusr}&password=${dbpwd}" 
	
	def grantPublic = null
	def addAdditionalAttributes = false
		
	def runImport(importDirectory) {
		EhiLogger.getInstance().setTraceFilter(false);
		
		def config = ili2dbConfig()
		
		// This is a bit tricky:
		// Ili2db appends data with the --import option
		// when the tables exists. This behaviour corresponds
		// with 'appending data'. Using the classes here 
		// in our own java code we need to set 
		// config.setConfigReadFromDb(true). Without this
		// ilidb tries to insert some meta data into
		// some tables but fails because of primary keys
		// constraints.
		config.setConfigReadFromDb(true)
		
//		new File(importDirectory).eachFile(FILES) {file ->
		
		new File(importDirectory).listFiles().sort{ it.name}.each() {file ->
			def fileName = file.getName()
			def fileExtension =  FilenameUtils.getExtension(fileName)
			
			if (!fileExtension.equalsIgnoreCase('itf')) {
				return
			}
			
			log.debug "Importing: ${file.getAbsolutePath()}"
						
			def startTime = Calendar.instance.timeInMillis
			
			config.setXtffile(file.getAbsolutePath())
			
			def fosnr = fileName.substring(3,7) as int // Exception will be thrown and no import will be done.
			def lot = fileName.substring(7,9) as int
			def today = new Date().toTimestamp() // Convert java.util.Date to java.sql.Date
			
			config.setLogfile("/home/stefan/tmp/ili2pg/${fosnr}_${lot}.log");
						
			try {
				Ili2db.runImport(config, "")
				
				// When an exception is thrown we should abort the import. 
				// There will be huge problems when we try to update
				// these columns with the next ITF because we assign a false fosnr etc. etc.
				// We could try a manual rollback though...
				
				// TODO: Check if exception throwing is enterprise ready :-)
				if (addAdditionalAttributes) {
					updateAdditionalColumns(fosnr, lot, today)
				}
			} catch (Ili2dbException e) {
				e.printStackTrace();
				log.error e.getMessage()
			} catch (Exception e) {
				e.printStackTrace();
				log.error e.getMessage()
//				throw new Exception(e)
			}
			
			def elapsedTime = (Calendar.instance.timeInMillis - startTime) / 1000
			log.debug "Importing done in: ${elapsedTime} s"
		}
		log.debug "All files imported."
	}
	
	private def updateAdditionalColumns(fosnr, lot, today) {
		def sql = Sql.newInstance(dburl)
		sql.connection.autoCommit = false
					
		try {
			def query = "SELECT * FROM ${Sql.expand(dbschema)}.t_ili2db_classname;"
			sql.eachRow(query) {row ->
				def tableName = row.sqlname.toLowerCase()
								
				def existsQuery = "SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = '${Sql.expand(dbschema)}' " +
								"AND table_name = '${Sql.expand(tableName)}');"
				if (sql.firstRow(existsQuery).exists) {
					def updateFosNrQuery = "UPDATE ${Sql.expand(dbschema)}.${Sql.expand(tableName)} " + 
									"SET (gem_bfs, los, lieferdatum) = (${fosnr}, ${lot}, ${today}) " + 
									"WHERE gem_bfs IS NULL;"
					sql.execute(updateFosNrQuery)
					
					log.trace "Adding additional attributes from table '${tableName}' are updated."
				}	
			}
			sql.commit()
		} catch (SQLException e) {
			sql.rollback()
			log.error e.getMessage()
			throw new SQLException(e)
		} finally {
			sql.connection.close()
			sql.close()
		}
	}
	
	def initSchema() {
		def config = ili2dbConfig()
		Ili2db.runSchemaImport(config, "");
		
		// Grant usage rights on schema and select on all tables in this schema
		// to a read only user (which must exist in the database).		
		if (grantPublic) {
			def sql = Sql.newInstance(dburl)
			sql.connection.autoCommit = false
			
			try {
				def query = "GRANT USAGE ON SCHEMA ${Sql.expand(dbschema)} TO ${Sql.expand(grantPublic)};" +
							"GRANT SELECT ON ALL TABLES IN SCHEMA ${Sql.expand(dbschema)} TO ${Sql.expand(grantPublic)};"
				sql.execute(query)
				sql.commit()
			} catch (SQLException e) {
				sql.rollback()
				log.error e.getMessage()
				throw new SQLException(e) // catch this in main method (I guess)
			} finally {
				sql.connection.close()  
				sql.close()
			}
			log.debug "Usage on schema and tables granted."
		} 
		
		// Add some additional attributes to the tables:
		// "gem_bfs" -> fosnr
		// "los" -> lot
		// "lieferdatum -> delivery date
		if (addAdditionalAttributes) {
			def sql = Sql.newInstance(dburl)
			sql.connection.autoCommit = false
			
			try {
				def query = "SELECT * FROM ${Sql.expand(dbschema)}.t_ili2db_classname;"
				sql.eachRow(query) {row ->
					def tableName = row.sqlname.toLowerCase() 					
					
					// There is more in the table t_ili2db_classname than just 'all' tables. 
					// We need to find out if an entry of t_ili2db is really a postgresql table.
					// TODO: Ask C. Eisenhut if there is a way to get only the table names?
					def existsQuery = "SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = '${Sql.expand(dbschema)}' " + 
									"AND table_name = '${Sql.expand(tableName)}');"
					if (sql.firstRow(existsQuery).exists) {
						def alterQuery = "ALTER TABLE ${Sql.expand(dbschema)}.${Sql.expand(tableName)} ADD COLUMN gem_bfs integer;" +
										"ALTER TABLE ${Sql.expand(dbschema)}.${Sql.expand(tableName)} ADD COLUMN los integer;" +
										"ALTER TABLE ${Sql.expand(dbschema)}.${Sql.expand(tableName)} ADD COLUMN lieferdatum date;"
						sql.execute(alterQuery)

						def indexQuery = "CREATE INDEX idx_${Sql.expand(tableName)}_gem_bfs " +
										"ON ${Sql.expand(dbschema)}.${Sql.expand(tableName)} USING btree(gem_bfs);"
						sql.execute(indexQuery)
							
						log.trace "Adding additional attributes to table: ${tableName}"
					}	
				}
				sql.commit()
			} catch (SQLException e) {
				sql.rollback()
				log.error e.getMessage()
				throw new SQLException(e)  
			} finally {
				sql.connection.close()
				sql.close()
			}
			log.debug "Additional attributes added to database tables."
		}
		log.debug "Schema created: ${dbschema}."		
	}
	
	private def ili2dbConfig() {
		def config = new Config()
		config.setDbhost(dbhost)
		config.setDbdatabase(dbdatabase)
		config.setDbport(dbport)
		config.setDbusr(dbusr)
		config.setDbpwd(dbpwd)
		config.setDbschema(dbschema)
		config.setDburl(dburl)
		
		config.setModels(modelName);
		config.setModeldir("http://models.geo.admin.ch/");
		
		config.setGeometryConverter(PostgisGeometryConverter.class.getName())
		config.setDdlGenerator(GeneratorPostgresql.class.getName())
		config.setJdbcDriver("org.postgresql.Driver")

		config.setNameOptimization("topic")
		config.setMaxSqlNameLength("60")
//		config.setStrokeArcs("enable")
						
		config.setSqlNull("enable") // be less restrictive
		config.setValue("ch.ehi.sqlgen.createGeomIndex", "True");
		config.setCreateEnumCols("addTxtCol")
				
		config.setDefaultSrsAuthority("EPSG")
		config.setDefaultSrsCode("21781")
		
		// Does not work. Do we want to have this working?
		// We write logfile of our own. I think there is no
		// easy way to have only one logfile?
		// TODO: Try to use ehi logger. Ask C. Eisenhut.
		EhiLogger.getInstance().setTraceFilter(false);
	
		return config
	}
}
