#!/bin/bash
AWS=/usr/bin/aws
PYTHON=/usr/bin/python
SPARK=/usr/bin/spark-submit

source shell_tools.sh

function run_spark(){
	pyscript=$1
	args=$2

	${SPARK} \
		--master yarn \
		--deploy-mode cluster \
		--driver-memory 10g \
		--executor-memory 2g \
		--num-executors 100 \
		--executor-cores 1 \
		--conf spark.driver.maxResultSize=10g \
		--conf spark.task.maxFailures=8 \
		--conf spark.default.parallelism=1000 \
		--conf spark.dynamicAllocation.maxExecutors=200 \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.yarn.maxAppAttempts=1 \
    --conf spark.executor.memoryOverhead=10G \
    --conf spark.executor.heartbeatInterval=30s\
    --conf spark.yarn.nodemanager.localizer.cache.target-size-mb=4g \
    --conf spark.yarn.nodemanager.localizer.cache.cleanup.interval-ms=300000 \
    --files s3://mx-machine-learning/luomingjun/others/spark/log4j.properties \
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties \
    --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties \
		--name "researcher: daily_active_user_num ${args}" \
		${pyscript} ${args}
 }


function check(){
	status=$1
	warning_msg=$2
	if [ ${status} -ne 0 ]; then
		echo -e  "${warning_msg} ${warning_sub}"
		return 1
	fi
	return 0
}


function run_how_many_user(){
  date=$1

  run_spark "how_many_user.py" "  --date ${date}  --version 1"
 	check $? "researcher: daily_active_user_num"
	if [ $? -ne 0 ]; then
		return 1
	fi
	return 0
}

#YMD='20210421'
YMD=$(date +%Y%m%d -u)
echo ${YMD}
run_how_many_user ${YMD}
