package org.catais.av

import groovy.util.logging.Log4j2

@Log4j2
class Utils {
	
	static elapsedTime(startTimeMs) {
		return  Calendar.instance.timeInMillis - startTimeMs
	}

}
