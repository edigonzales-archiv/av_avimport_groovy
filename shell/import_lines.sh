#!/bin/bash
for FILE in /home/stefan/Downloads/av_ch/*.itf
do
  BASENAME=$(basename $FILE .itf)
  echo "Processing: ${BASENAME}.itf"

  java -jar  /home/stefan/Apps/ili2pg-2.2.0/ili2pg.jar --import --dbdatabase xanadu2 --dbusr stefan --dbpwd ziegler12 --createGeomIdx --nameByTopic --sqlEnableNull --skipPolygonBuilding --keepAreaRef --dbschema av_avdpool_ng_lines $FILE

done

