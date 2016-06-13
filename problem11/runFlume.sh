#sudo mkdir -p /tmp/spooldir
#sudo chmod a+w -R /tmp
#./copy-move-weblogs.sh /tmp/spooldir/

flume-ng agent \
--conf /etc/flume-ng/conf \
--conf-file ~/flume.conf \
--name agent1 -Dflume.root.logger=INFO,console
