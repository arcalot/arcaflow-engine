SHELL := /bin/bash

export module_name = arcaflow
export linux_amd64 = manylinux2014_x86_64
export linux_arm64 = manylinux2014_aarch64
export darwin_amd64 = macosx_10_9_x86_64
export darwin_arm64 = macosx_11_0_arm64

BIN_PATH=${CURDIR}/${module_name}/bin
ARTIFACTS_PATH=${CURDIR}/artifacts
BUILD_PATH=${CURDIR}/build

# help
help:
	@echo "Usage: make setup && make all"

setup: 
	pip3.9 install wheel setuptools twine


build_unix: 
	declare -a unix_archs=("linux_amd64" "linux_arm64" "darwin_amd64" "darwin_arm64") ;\
	for i in $${unix_archs[@]};\
	do cp ${ARTIFACTS_PATH}/arcaflow_$${i}*/arcaflow ${BIN_PATH};\
	chmod +x ${BIN_PATH}/arcaflow;\
	python3.9 setup.py bdist_wheel --plat-name $${!i};\
	rm -rf ${BIN_PATH}/*;\
	rm -rf ${BUILD_PATH}/*;\
	done;\

build_windows:
	cp ${ARTIFACTS_PATH}/arcaflow_windows_amd64*/arcaflow.exe ${BIN_PATH}
	python3.9 setup.py bdist_wheel --plat-name win_amd64
	rm -rf ${BIN_PATH}/*
	rm -rf ${BUILD_PATH}/*

all: build_unix build_windows

clean:
	rm -rf ${CURDIR}/dist/*
	rm -rf ${BIN_PATH}/*
	rm -rf ${CURDIR}/*egg-info
	rm -rf ${BUILD_PATH}/*

