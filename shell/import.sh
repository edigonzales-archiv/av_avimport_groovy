#!/bin/bash
for FILE in /home/stefan/Downloads/av_ch/*.itf
do
  BASENAME=$(basename $FILE .itf)
  echo "Processing: ${BASENAME}.itf"
  #java -jar  /home/stefan/Apps/ili2pg-2.2.0/ili2pg.jar --trace --schemaimport --dbdatabase xanadu2 --dbusr stefan --dbpwd ziegler12 --models DM01AVCH24D --createGeomIdx --nameByTopic --sqlEnableNull --dbschema av_avdpool_ng
  java -jar  /home/stefan/Apps/ili2pg-2.2.0/ili2pg.jar --trace --log ./ili2pg.log --import --dbdatabase xanadu2 --dbusr stefan --dbpwd ziegler12 --createGeomIdx --nameByTopic --sqlEnableNull --dbschema av_avdpool_ng $FILE

done

