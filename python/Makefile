SHELL := /bin/bash

export module_name = arcaflow
export linux_amd64 = manylinux2014_x86_64
export linux_arm64 = manylinux2014_aarch64
export darwin_amd64 = macosx_10_9_x86_64
export darwin_arm64 = macosx_11_0_arm64

BIN_PATH=${CURDIR}/$${module_name}/bin

# help
help:
	@echo "Usage: make setup && make all"

setup: 
	pip3.9 install wheel setuptools twine

unarchive_binaries:
	@for i in `ls artifacts/*tar.gz`;do FOLDER_NAME=`echo $${i} | sed -r "s/arcaflow_.*_(.*_.*)\.tar\.gz/\1/"`;mkdir $${FOLDER_NAME};tar xfvz $${i} -C $${FOLDER_NAME}; done
	@for i in `ls artifacts/*zip`;do FOLDER_NAME=`echo $${i} | sed -r "s/arcaflow_.*_(.*_.*)\.zip/\1/"`;mkdir $${FOLDER_NAME};unzip $${i} -d $${FOLDER_NAME}; done


build_all: build_windows
	declare -a unix_archs=("linux_amd64" "linux_arm64" "darwin_amd64" "darwin_arm64") ;\
	for i in $${unix_archs[@]};\
	do cp artifacts/$${i}/arcaflow ${BIN_PATH};\
	python3.9 setup.py bdist_wheel --plat-name $${!i};\
	rm -rf ${BIN_PATH}/*;\
	done;\

build_windows:
	cp artifacts/windows_amd64/arcaflow.exe ${BIN_PATH}
	python3.9 setup.py bdist_wheel --plat-name win_amd64
	rm -rf ${BIN_PATH}/*

all: unarchive_binaries build_all

clean:
	rm -rf dist/*
	rm -rf ${BIN_PATH}/*
	rm -rf artifacts
	rm -rf *egg-info
	rm -rf build/*



	