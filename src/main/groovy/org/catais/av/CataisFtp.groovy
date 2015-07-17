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
				
				println fileName
				fileList << fileName
				
//				def incomingFile = new File()
				
			}
		
			return fileList
		}
		
		// We can use this list to do some QA (e.g. compare list from ftp server with list of downloaded files etc.)
		println fileList
		
	}	
}
