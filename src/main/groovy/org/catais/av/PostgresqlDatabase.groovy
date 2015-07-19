package org.catais.av

import groovy.util.logging.Log4j2
import groovy.sql.Sql
import java.sql.SQLException
import static groovy.io.FileType.*
import static groovy.io.FileVisitResult.*
import org.apache.commons.io.FilenameUtils

import ch.ehi.ili2db.base.Ili2db
import ch.ehi.ili2db.base.Ili2dbException
import ch.ehi.ili2db.gui.Config
import ch.ehi.ili2pg.converter.PostgisGeometryConverter
import ch.ehi.sqlgen.generator_impl.jdbc.GeneratorPostgresql

@Log4j2
class PostgresqlDatabase {
	def dbhost = "localhost"
	def dbport = "5432"
	def dbdatabase = "xanadu2"
	def dbusr = "stefan"
	def dbpwd = "ziegler12"
	def dbschema = "av_avdool_ng"
	def modelName = "DM01AVCH24D"
	def dburl = "jdbc:postgresql://${dbhost}:${dbport}/${dbdatabase}"
	
	def grantPublic = null
	def addAdditionalAttributes = false
	
	def runImport(importDirectory) {
		
		def config = ili2dbConfig()
		
		// This is a bit tricky:
		// Ili2db appends data with the --import option
		// when the tables exists. It seems that it tries
		// to write some records into t_ili2db_settings 
		// table which is not necessary when we do actually
		// an appending of data. It throws an 'duplicate key'
		// error. With .setConfigReadFromDb(true) ili2db
		// does not try to write data into t_ili2db_settings.
		//
		// At the moment we need to run .schemaImport once!
		//
		// TODO: Ask C. Eisenhut.
		config.setConfigReadFromDb(true)
		
		new File(importDirectory).eachFile(FILES) {file ->
			def fileName = file.getName()
			def fileExtension =  FilenameUtils.getExtension(fileName)
			
			if (!fileExtension.equalsIgnoreCase('itf')) {
				return
			}
			
			log.debug "Importing: ${file.getAbsolutePath()}"
			def startTime = Calendar.instance.timeInMillis
	
			
			config.setXtffile(file.getAbsolutePath())
			
			try {
				Ili2db.runImport(config, "")
			}
			catch (Exception e) {
				e.printStackTrace();
				log.error e.getMessage()
				
			} catch (Ili2dbException e) {
				e.printStackTrace();
				log.error e.getMessage()
			}
			
			def endTime = Calendar.instance.timeInMillis
			def elapsedTime = (endTime - startTime) / 1000
			log.debug "Importing done in: ${elapsedTime} s"
		}
		log.info "All files imported."
	}
		
	def createSchema() {
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
				sql.connection.close() // this is really executed even we throw a new exception
				sql.close()
			}
			
			log.debug "Usage on schema and tables granted."
		} 
		
		// Add some additional attributes to the tables.
		// fosnr, lot and delivery date.
		if (addAdditionalAttributes) {
			def sql = Sql.newInstance(dburl)
			sql.connection.autoCommit = false
			
			try {
				def query = "SELECT * FROM ${Sql.expand(dbschema)}.t_ili2db_classname;"
				sql.eachRow(query) {row ->
					def tableName = row.sqlname.toLowerCase() 					
					
					// In the table t_ili2db_classname is more than just 'all' tables. 
					// We need to find out if an entry of t_ili2db is really a postgresql table.
					// TODO: Ask C. Eisenhut if there is a way to get only the table names?
					def existsQuery = "SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = '${Sql.expand(dbschema)}' AND table_name = '${Sql.expand(tableName)}');"
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
		log.info "Schema created: ${dbschema}."		
	}
	
	private def ili2dbConfig() {
		def config = new Config()
		config.setDbhost(dbhost)
		config.setDbdatabase(dbdatabase)
		config.setDbport(dbport)
		config.setDbusr(dbusr)
		config.setDbpwd(dbpwd)
		config.setDbschema(dbschema)
		config.setModels(modelName);
		config.setModeldir("http://models.geo.admin.ch/");
		
		config.setGeometryConverter(PostgisGeometryConverter.class.getName())
		config.setDdlGenerator(GeneratorPostgresql.class.getName())
		config.setJdbcDriver("org.postgresql.Driver")

		config.setNameOptimization("topic")
		config.setMaxSqlNameLength("60")
		config.setStrokeArcs("enable")
		config.setSqlNull("enable"); // be less restrictive
		config.setValue("ch.ehi.sqlgen.createGeomIndex", "True");
		
		// TODO: Would it make sense to create an index on pk (t_id) and fk?
		
		config.setDefaultSrsAuthority("EPSG")
		config.setDefaultSrsCode("21781")
		
		config.setDburl(dburl)
		
		config.setLogfile("/Users/stefan/tmp/foo.log");
	
		return config
	}
}
