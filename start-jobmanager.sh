active_dir=$1
img_dir=$HOME/diaspora/src/flink
apptainer instance start --cleanenv --fakeroot --writable-tmpfs \
	--bind $active_dir/log:/opt/flink/log \
	--bind $active_dir/configs/jobmanager-config.yaml:/opt/flink/conf/config.yaml \
	--bind $active_dir/workloads:/opt/workloads \
	--bind /soft/xalt/:/soft/xalt/ \
	$img_dir/flink_img flink-jobmanager

apptainer exec --fakeroot --cleanenv \
	instance://flink-jobmanager /opt/flink/bin/jobmanager.sh start-foreground \
	> /dev/null 2> /dev/null


