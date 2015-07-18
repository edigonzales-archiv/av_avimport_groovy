package org.catais.av

import groovy.util.logging.Log4j2
import org.apache.commons.net.ftp.FTPClient
import org.apache.commons.io.FilenameUtils

@Log4j2
class CataisFtp {
	String url = 'www.catais.org'
//	String url = 'ftp.infogrips.ch' // use .listNames() to obtain file names: http://commons.apache.org/proper/commons-net/javadocs/api-3.3/index.html
	String usr = 'anonymous'
	String pwd = ''
	String workingDirectory = '/geodaten/ch/so/agi/av/dm01avch24d/itf/lv03/'
	String downloadDirectory = '/tmp/'
	
	def downloadData() {
		def fileList = new FTPClient().with {			
			connect url
			enterLocalPassiveMode()
			login usr, pwd
			changeWorkingDirectory workingDirectory
			
			def fileList = []
			
			listFiles().each {file ->
				def fileName = file.getName()
				def fileExtension =  FilenameUtils.getExtension(fileName)
				
				if (fileExtension == 'zip') {
					return
				}
				
				log.info "Downloading: ${fileName}"
				def incomingFile = new File(downloadDirectory + File.separator + fileName)
				if (!incomingFile.withOutputStream {ostream -> retrieveFile fileName, ostream}) {
					log.error 'Error while downloading data.'
					return
				}
			
				log.debug "File downloaded: ${incomingFile.absoluteFile}"
				fileList << incomingFile.absoluteFile
			}
			
			return fileList
		}
		return fileList		
	}	
}
