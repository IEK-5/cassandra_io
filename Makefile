init:
	mkdir -p temp
	TMPDIR=./temp pip3 install -r requirements.txt
	TMPDIR=./temp pip3 install -e .
	rmdir temp

.PHONY: init
