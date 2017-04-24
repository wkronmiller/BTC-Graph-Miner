all: src/main.c
	mpicc -g -I. -Wall -O3 src/main.c -o a.out
