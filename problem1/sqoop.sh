sqoop import \
	--options-file pass.txt \
	--table calllog
	--target-dir /user/training/problem1/output
	--fields-terminated-by '\t'
