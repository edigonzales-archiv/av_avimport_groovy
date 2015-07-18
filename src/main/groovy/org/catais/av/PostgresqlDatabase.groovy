package org.catais.av

import groovy.util.logging.Log4j2
import groovy.sql.Sql
import java.sql.SQLException

import ch.ehi.ili2db.base.Ili2db
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
	
	def publicDbusr = "mspublic"
	def grantSelectToPublicDbusr = false
	def addAdditionalAttributes = false
	
	def createSchema() {
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
		
		// TODO: Geometry Index????
		
		config.setDefaultSrsAuthority("EPSG")
		config.setDefaultSrsCode("21781")
		
		def dburl = "jdbc:postgresql://${dbhost}:${dbport}/${dbdatabase}"
		config.setDburl(dburl)

		Ili2db.runSchemaImport(config, "");
		log.info "Schema created: ${dbschema}."
		
		// Grant usage rights on schema an select on all tables in this schema
		// to a read only user (which must exist in the database).
		if (grantSelectToPublicDbusr) {
			def sql = Sql.newInstance(dburl)
			sql.connection.autoCommit = false
			
			try {
				def query = "GRANT USAGE ON SCHEMA ${Sql.expand(dbschema)} TO ${Sql.expand(publicDbusr)};" +
							"GRANT SELECT ON ALL TABLES IN SCHEMA ${Sql.expand(dbschema)} TO ${Sql.expand(publicDbusr)};"
				sql.execute(query)
				sql.commit()
			} catch (SQLException e) {
				sql.rollback()
				log.error e.getMessage()
				throw new SQLException(e) // catch this in main method (I guess)
			} finally {
				sql.connection.close() // this is executed even we throw a new exception
				sql.close()
			}
			
			log.info "Usage on schema and tables granted."
		} 
		
		// Add some additional attributes to the tables.
		// E.g. fosnr and delivery date.
		if (addAdditionalAttributes) {
			def sql = Sql.newInstance(dburl)
			sql.connection.autoCommit = false
			
			try {
				def query = "SELECT * FROM ${Sql.expand(dbschema)}.t_ili2db_classname;"
				sql.eachRow(query) {row ->
					def tableName = row.sqlname.toLowerCase() 					
					
					// In the table t_ili2db_classname is more than just 'all' tables. 
					// We need to find out if an entry of t_ili2db is really a postgresql table.
					def existsQuery = "SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = '${Sql.expand(dbschema)}' AND table_name = '${Sql.expand(tableName)}');"
					if (sql.firstRow(existsQuery).exists) {
						def alterQuery = "ALTER TABLE ${Sql.expand(dbschema)}.${Sql.expand(tableName)} ADD COLUMN gem_bfs integer;" +
										"ALTER TABLE ${Sql.expand(dbschema)}.${Sql.expand(tableName)} ADD COLUMN los integer;" +
										"ALTER TABLE ${Sql.expand(dbschema)}.${Sql.expand(tableName)} ADD COLUMN lieferdatum date;"
						sql.execute(alterQuery)
						
						log.debug "Adding additional attributes to table: ${tableName}"
						
						// TODO: add index for gem_bfs!!
					}	
				}
			} catch (SQLException e) {
				sql.rollback()
				log.error e.getMessage()
				throw new SQLException(e)  
			} finally {
				sql.connection.close()
				sql.close()
			}
					
			log.info "Additional attributes added to database tables."
		}
		
				
	}

}
